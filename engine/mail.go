package engine

import (
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// MailManager 邮件管理器
type MailManager struct {
	engine *Engine
	logger *zap.Logger
	config *MailConfig
}

// MailConfig 邮件配置
type MailConfig struct {
	SMTPHostname   string
	SMTPPort       int
	From           string
	Username       string
	Password       string
	MaxPerDay      int
	TLSMode        string
	TimeoutSeconds int
	TLSServerName  string
	TLSCAFile      string
}

// NewMailManager 创建邮件管理器
func NewMailManager(engine *Engine, logger *zap.Logger) *MailManager {
	smtpHost := engine.config.SMTPHostname
	if smtpHost == "" {
		smtpHost = "localhost"
	}
	smtpPort := engine.config.SMTPPort
	if smtpPort == 0 {
		smtpPort = 25
	}
	from := engine.config.EmailFrom
	if from == "" {
		from = "admin@localhost"
	}
	return &MailManager{
		engine: engine,
		logger: logger,
		config: &MailConfig{
			SMTPHostname:   smtpHost,
			SMTPPort:       smtpPort,
			From:           from,
			MaxPerDay:      engine.config.MaxEmailsPerDay,
			Username:       engine.config.SMTPUsername,
			Password:       engine.config.SMTPPassword,
			TLSMode:        engine.config.SMTPTLSMode,
			TimeoutSeconds: engine.config.SMTPTimeoutSeconds,
			TLSServerName:  engine.config.SMTPTLSServerName,
			TLSCAFile:      engine.config.SMTPTLSCAFile,
		},
	}
}

// SendJobSuccessEmail 发送任务成功邮件
func (mm *MailManager) SendJobSuccessEmail(job *types.Job, recipients string) error {
	if recipients == "" {
		return nil
	}

	template := `Job completed successfully

Job ID: {{.ID}}
Event: {{.EventTitle}}
Hostname: {{.Hostname}}
Elapsed: {{.Elapsed}} seconds
Description: {{.Description}}

View details: {{.JobDetailsURL}}
`

	data := mm.prepareEmailData(job)
	return mm.sendEmail(recipients, "Job Completed Successfully", template, data)
}

// SendJobFailureEmail 发送任务失败邮件
func (mm *MailManager) SendJobFailureEmail(job *types.Job, recipients string) error {
	if recipients == "" {
		return nil
	}

	template := `Job failed

Job ID: {{.ID}}
Event: {{.EventTitle}}
Hostname: {{.Hostname}}
Error Code: {{.Code}}
Error: {{.Description}}
Elapsed: {{.Elapsed}} seconds

View details: {{.JobDetailsURL}}
Edit event: {{.EditEventURL}}
`

	data := mm.prepareEmailData(job)
	return mm.sendEmail(recipients, "Job Failed", template, data)
}

// SendEventErrorEmail 发送事件错误邮件
func (mm *MailManager) SendEventErrorEmail(event *types.Event, description string, recipients string) error {
	if recipients == "" {
		return nil
	}

	template := `Event Error

Event: {{.EventTitle}}
Error: {{.Description}}

Edit event: {{.EditEventURL}}
`

	data := map[string]interface{}{
		"EventTitle":   event.Title,
		"Description":  description,
		"EditEventURL": mm.engine.config.BaseAppURL + "/#Schedule?sub=edit_event&id=" + event.ID,
	}

	return mm.sendEmail(recipients, "Event Error", template, data)
}

// prepareEmailData 准备邮件数据
func (mm *MailManager) prepareEmailData(job *types.Job) map[string]interface{} {
	return map[string]interface{}{
		"ID":            job.ID,
		"EventTitle":    job.EventTitle,
		"Hostname":      job.Hostname,
		"Code":          job.Code,
		"Description":   job.Description,
		"Elapsed":       job.Elapsed,
		"JobDetailsURL": mm.engine.config.BaseAppURL + "/#JobDetails?id=" + job.ID,
		"EditEventURL":  mm.engine.config.BaseAppURL + "/#Schedule?sub=edit_event&id=" + job.Event,
	}
}
