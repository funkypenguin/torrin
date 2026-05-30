package nntp

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"time"
)

type Conn struct {
	conn     net.Conn
	reader   *textproto.Reader
	writer   *bufio.Writer
	LastUsed time.Time
}

func Dial(creds *Credentials) (*Conn, error) {
	addr := fmt.Sprintf("%s:%d", creds.Host, creds.Port)

	var rawConn net.Conn
	var err error

	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}

	if creds.SSL {
		rawConn, err = tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
			MinVersion: tls.VersionTLS12,
		})
	} else {
		rawConn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	c := &Conn{
		conn:     rawConn,
		reader:   textproto.NewReader(bufio.NewReaderSize(rawConn, 512*1024)),
		writer:   bufio.NewWriterSize(rawConn, 64*1024),
		LastUsed: time.Now(),
	}

	code, _, err := c.readResponse()
	if err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("greeting: %w", err)
	}
	if code != 200 && code != 201 {
		rawConn.Close()
		return nil, fmt.Errorf("unexpected greeting: %d", code)
	}

	if creds.Username != "" {
		if err := c.auth(creds.Username, creds.Password); err != nil {
			rawConn.Close()
			return nil, err
		}
	}

	return c, nil
}

func (c *Conn) auth(username, password string) error {
	code, msg, err := c.command("AUTHINFO USER %s", username)
	if err != nil {
		return fmt.Errorf("auth user: %w", err)
	}
	if code/100 == 2 {
		return nil
	}
	if code != 381 && code != 350 {
		return fmt.Errorf("auth user: %d %s", code, msg)
	}

	code, msg, err = c.command("AUTHINFO PASS %s", password)
	if err != nil {
		return fmt.Errorf("auth pass: %w", err)
	}
	if code/100 != 2 {
		return fmt.Errorf("auth failed: %d %s", code, msg)
	}
	return nil
}

func (c *Conn) Group(name string) error {
	c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer c.conn.SetDeadline(time.Time{})

	code, msg, err := c.command("GROUP %s", name)
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return fmt.Errorf("group %s: %d %s", name, code, msg)
	}
	c.LastUsed = time.Now()
	return nil
}

func (c *Conn) Body(messageID string) ([]byte, error) {
	c.conn.SetDeadline(time.Now().Add(60 * time.Second))
	defer c.conn.SetDeadline(time.Time{})

	code, msg, err := c.command("BODY <%s>", messageID)
	if err != nil {
		return nil, fmt.Errorf("body: %w", err)
	}
	if code != 222 {
		return nil, fmt.Errorf("body %s: %d %s", messageID, code, msg)
	}

	body, err := c.reader.ReadDotBytes()
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	c.LastUsed = time.Now()
	return body, nil
}

func (c *Conn) Stat(messageID string) error {
	c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer c.conn.SetDeadline(time.Time{})

	code, msg, err := c.command("STAT <%s>", messageID)
	if err != nil {
		return err
	}
	if code != 223 {
		return fmt.Errorf("stat %s: %d %s", messageID, code, msg)
	}
	c.LastUsed = time.Now()
	return nil
}

func (c *Conn) Ping() error {
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer c.conn.SetDeadline(time.Time{})

	code, _, err := c.command("DATE")
	if err != nil {
		return err
	}
	if code != 111 {
		return fmt.Errorf("ping: unexpected %d", code)
	}
	c.LastUsed = time.Now()
	return nil
}

func (c *Conn) Close() error {
	c.command("QUIT")
	return c.conn.Close()
}

func (c *Conn) command(format string, args ...any) (int, string, error) {
	cmd := fmt.Sprintf(format, args...)
	if _, err := io.WriteString(c.writer, cmd+"\r\n"); err != nil {
		return 0, "", err
	}
	if err := c.writer.Flush(); err != nil {
		return 0, "", err
	}
	return c.readResponse()
}

func (c *Conn) readResponse() (int, string, error) {
	line, err := c.reader.ReadLine()
	if err != nil {
		return 0, "", err
	}
	if len(line) < 3 {
		return 0, "", fmt.Errorf("short response: %q", line)
	}
	code := 0
	for i := 0; i < 3; i++ {
		code = code*10 + int(line[i]-'0')
	}
	msg := ""
	if len(line) > 4 {
		msg = strings.TrimSpace(line[4:])
	}
	return code, msg, nil
}
