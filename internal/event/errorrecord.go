package event

import "encoding/json"

// ErrorRecord descreve um evento rejeitado. É o formato de cada linha do
// stderr: {"line":N,"stage":"...","error":"...","raw":"..."} ou, quando o
// evento já tinha sido parseado, "event" no lugar de "raw".
type ErrorRecord struct {
	Seq   uint64
	Stage string
	Err   error
	Raw   []byte // linha original, quando o erro é de parse
	Event *Event // evento, quando o erro é de um Stage
}

type errorRecordJSON struct {
	Line  uint64         `json:"line"`
	Stage string         `json:"stage"`
	Error string         `json:"error"`
	Raw   *string        `json:"raw,omitempty"`
	Event map[string]any `json:"event,omitempty"`
}

// AppendJSON acrescenta o registro em JSON (uma linha, sem "\n") a dst.
func (r ErrorRecord) AppendJSON(dst []byte) ([]byte, error) {
	j := errorRecordJSON{Line: r.Seq, Stage: r.Stage}
	if r.Err != nil {
		j.Error = r.Err.Error()
	}
	if r.Raw != nil {
		s := string(r.Raw) // UTF-8 inválido vira U+FFFD no Marshal
		j.Raw = &s
	}
	if r.Event != nil {
		j.Event = r.Event.Fields
	}
	b, err := json.Marshal(j)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}
