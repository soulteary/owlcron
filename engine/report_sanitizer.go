package engine

import (
	"bytes"
	"github.com/soulteary/owlcron/types"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"net/url"
	"strings"
)

// Preserve ordinary report formatting while discarding executable HTML, foreign
// namespaces, inline event handlers, style URLs, and unsafe link schemes.
func sanitizeReportHTML(content string) string {
	context := &html.Node{Type: html.ElementNode, Data: "div", DataAtom: atom.Div}
	nodes, err := html.ParseFragment(strings.NewReader(content), context)
	if err != nil {
		return ""
	}
	allowed := map[string]bool{}
	for _, tag := range strings.Fields("address article aside footer header h1 h2 h3 h4 h5 h6 hgroup main nav section blockquote dd div dl dt figcaption figure hr li ol p pre ul a abbr b bdi bdo br cite code data dfn em i kbd mark q rb rp rt rtc ruby s samp small span strong sub sup time u var wbr caption col colgroup table tbody td tfoot th thead tr") {
		allowed[tag] = true
	}
	forbidden := map[string]bool{"script": true, "style": true, "iframe": true, "object": true, "embed": true, "svg": true, "math": true, "template": true}
	var clean func(*html.Node) *html.Node
	clean = func(node *html.Node) *html.Node {
		if node.Type == html.TextNode {
			return &html.Node{Type: html.TextNode, Data: node.Data}
		}
		if node.Type != html.ElementNode || node.Namespace != "" || forbidden[node.Data] {
			return nil
		}
		out := &html.Node{Type: html.ElementNode, Data: node.Data, DataAtom: node.DataAtom}
		if !allowed[node.Data] {
			out = &html.Node{Type: html.ElementNode, Data: "span", DataAtom: atom.Span}
		}
		for _, attr := range node.Attr {
			if attr.Namespace != "" {
				continue
			}
			switch attr.Key {
			case "class":
				if node.Data == "div" || node.Data == "pre" || node.Data == "code" {
					out.Attr = append(out.Attr, attr)
				}
			case "href":
				if node.Data == "a" {
					link, err := url.Parse(strings.TrimSpace(attr.Val))
					if err == nil && (link.Scheme == "" || strings.EqualFold(link.Scheme, "http") || strings.EqualFold(link.Scheme, "https") || strings.EqualFold(link.Scheme, "mailto")) {
						out.Attr = append(out.Attr, html.Attribute{Key: "href", Val: link.String()})
					}
				}
			case "target":
				if node.Data == "a" && attr.Val == "_blank" {
					out.Attr = append(out.Attr, attr, html.Attribute{Key: "rel", Val: "noopener noreferrer"})
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			if safe := clean(child); safe != nil {
				out.AppendChild(safe)
			}
		}
		return out
	}
	var buf bytes.Buffer
	for _, node := range nodes {
		if safe := clean(node); safe != nil {
			_ = html.Render(&buf, safe)
		}
	}
	return buf.String()
}
func sanitizeJobReports(job *types.Job) {
	if report, ok := job.HTML.(map[string]interface{}); ok {
		for _, key := range []string{"title", "content", "caption"} {
			if value, ok := report[key].(string); ok {
				report[key] = sanitizeReportHTML(value)
			}
		}
	}
	if table, ok := job.Table.(map[string]interface{}); ok {
		for _, key := range []string{"title", "caption"} {
			if value, ok := table[key].(string); ok {
				table[key] = sanitizeReportHTML(value)
			}
		}
		for _, key := range []string{"header", "rows"} {
			if values, ok := table[key].([]interface{}); ok {
				for i, value := range values {
					switch cell := value.(type) {
					case string:
						values[i] = sanitizeReportHTML(cell)
					case []interface{}:
						for j, value := range cell {
							if text, ok := value.(string); ok {
								cell[j] = sanitizeReportHTML(text)
							}
						}
					}
				}
			}
		}
	}
}
