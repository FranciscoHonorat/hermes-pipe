// Package alias esconde o pacote time atrás de um alias de import.
package alias

import t "time"

func Stamp() int64 {
	return t.Now().Unix() // want "time.Now"
}
