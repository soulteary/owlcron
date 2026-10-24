package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"reflect"
	"regexp"
	"strings"
)

const webhookRedacted = "[REDACTED]"

var sensitiveAssignment = regexp.MustCompile(`(?i)(\b(?:password|passwd|secret(?:_key)?|access_token|refresh_token|token|api[_-]?key|authorization|cookie)\s*[:=]\s*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s&,;]+)`)
var sensitiveBearer = regexp.MustCompile(`(?i)\b(Bearer|Basic)\s+[A-Za-z0-9+/=_\-.]+`)
var embeddedURL = regexp.MustCompile(`https?://[^\s<>"']+`)

func sensitiveWebHookKey(key string) bool {
	normalized := strings.ToLower(strings.Map(func(r rune) rune {
		if r == '_' || r == '-' || r == '.' || r == ' ' {
			return -1
		}
		return r
	}, key))
	for _, word := range []string{"password", "passwd", "secret", "token", "authorization", "apikey", "cookie", "credential", "privatekey"} {
		if strings.Contains(normalized, word) {
			return true
		}
	}
	return normalized == "auth"
}

// RedactSensitiveText removes configured credentials and common credential formats.
// Configuration is immutable after construction; no shared input is modified.
func (e *Engine) RedactSensitiveText(text string) string {
	if e != nil && e.nodeAuth != nil && len(e.nodeAuth.private) > 0 {
		for _, key := range [][]byte{e.nodeAuth.private, e.nodeAuth.private.Seed()} {
			text = replaceWebHookSecret(text, base64.StdEncoding.EncodeToString(key))
			text = replaceWebHookSecret(text, hex.EncodeToString(key))
		}
	}

	if e != nil && e.webHookSigner != nil && len(e.webHookSigner.secret) > 0 {
		text = replaceWebHookSecret(text, string(e.webHookSigner.secret))
	}
	if e != nil && e.config != nil {
		cfg := reflect.ValueOf(e.config).Elem()
		typ := cfg.Type()
		for i := 0; i < cfg.NumField(); i++ {
			if cfg.Field(i).Kind() == reflect.String && sensitiveWebHookKey(typ.Field(i).Name) {
				if secret := cfg.Field(i).String(); secret != "" {
					text = replaceWebHookSecret(text, secret)
				}
			}
		}
	}
	text = embeddedURL.ReplaceAllStringFunc(text, func(raw string) string {
		u, err := url.Parse(raw)
		if err != nil {
			return "[REDACTED URL]"
		}
		if u.User != nil {
			u.User = url.User(webhookRedacted)
		}
		q, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			u.RawQuery = "redacted"
		} else {
			for key := range q {
				if sensitiveWebHookKey(key) {
					q.Set(key, webhookRedacted)
				}
			}
			u.RawQuery = q.Encode()
		}
		u.Fragment = ""
		return u.String()
	})
	text = sensitiveBearer.ReplaceAllString(text, "$1 "+webhookRedacted)
	return sensitiveAssignment.ReplaceAllString(text, "${1}"+webhookRedacted)
}

// sanitizeWebHookData creates a detached JSON-compatible snapshot, including typed
// maps and structs. Depth and node limits bound hostile/cyclic custom payloads.
func (e *Engine) sanitizeWebHookData(data map[string]interface{}) map[string]interface{} {
	budget := 10000
	var walk func(reflect.Value, int) interface{}
	walk = func(v reflect.Value, depth int) interface{} {
		budget--
		if budget < 0 || depth > 24 {
			return webhookRedacted
		}
		if !v.IsValid() {
			return nil
		}
		if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return nil
			}
			return walk(v.Elem(), depth+1)
		}
		if v.CanInterface() {
			if number, ok := v.Interface().(json.Number); ok {
				return number
			}
			if raw, ok := v.Interface().(json.RawMessage); ok {
				var parsed interface{}
				if len(raw) > 1<<20 || decodeWebHookJSON(raw, &parsed) != nil {
					return webhookRedacted
				}
				return walk(reflect.ValueOf(parsed), depth+1)
			}
		}
		switch v.Kind() {
		case reflect.String:
			value := v.String()
			trimmed := strings.TrimSpace(value)
			if len(trimmed) <= 1<<20 && (strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")) {
				var parsed interface{}
				if decodeWebHookJSON([]byte(trimmed), &parsed) == nil {
					cleaned, err := json.Marshal(walk(reflect.ValueOf(parsed), depth+1))
					if err == nil {
						return string(cleaned)
					}
				}
			}
			return e.RedactSensitiveText(value)
		case reflect.Map:
			if v.IsNil() {
				return nil
			}
			if v.Type().Key().Kind() != reflect.String {
				return webhookRedacted
			}
			capacity := v.Len()
			if capacity > budget {
				capacity = budget
			}
			if capacity < 0 {
				capacity = 0
			}
			result := make(map[string]interface{}, capacity)
			iter := v.MapRange()
			for iter.Next() {
				budget--
				if budget < 0 {
					break
				}
				key := iter.Key().String()
				cleanKey := e.RedactSensitiveText(key)
				if sensitiveWebHookKey(key) {
					result[cleanKey] = webhookRedacted
				} else {
					result[cleanKey] = walk(iter.Value(), depth+1)
				}
				if budget < 0 {
					break
				}
			}
			return result
		case reflect.Slice, reflect.Array:
			if v.Kind() == reflect.Slice && v.IsNil() {
				return nil
			}
			size := v.Len()
			if size > budget {
				size = budget
			}
			if size < 0 {
				return webhookRedacted
			}
			result := make([]interface{}, 0, size)
			for i := 0; i < size; i++ {
				result = append(result, walk(v.Index(i), depth+1))
			}
			return result
		case reflect.Struct:
			result := make(map[string]interface{})
			for i := 0; i < v.NumField(); i++ {
				budget--
				if budget < 0 {
					break
				}
				field := v.Type().Field(i)
				if field.PkgPath != "" {
					continue
				}
				key := strings.Split(field.Tag.Get("json"), ",")[0]
				if key == "-" {
					continue
				}
				if key == "" {
					key = field.Name
				}
				if sensitiveWebHookKey(key) {
					result[key] = webhookRedacted
				} else {
					result[key] = walk(v.Field(i), depth+1)
				}
			}
			return result
		case reflect.Bool:
			return v.Bool()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return v.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return v.Uint()
		case reflect.Float32, reflect.Float64:
			return v.Float()
		default:
			return webhookRedacted
		}
	}
	cleaned, _ := walk(reflect.ValueOf(data), 0).(map[string]interface{})
	if cleaned == nil {
		return map[string]interface{}{}
	}
	return cleaned
}

// Escaped credentials are common in URL query values and error messages.
func replaceWebHookSecret(text, secret string) string {
	if secret == "" {
		return text
	}
	for _, encoded := range []string{secret, url.QueryEscape(secret), url.PathEscape(secret)} {
		text = strings.ReplaceAll(text, encoded, webhookRedacted)
	}
	return text
}

func decodeWebHookJSON(raw []byte, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("multiple JSON values")
	}
	return nil
}

func (e *Engine) cleanWebHookBody(raw []byte) ([]byte, error) {
	if len(raw) > 1<<20 {
		return nil, errors.New("webhook body exceeds redaction limit")
	}
	var data map[string]interface{}
	if err := decodeWebHookJSON(raw, &data); err != nil {
		return nil, err
	}
	return json.Marshal(e.sanitizeWebHookData(data))
}
