module github.com/ianic/iol

go 1.21.0

require (
	github.com/pawelgaczynski/giouring v0.0.0-20230821111130-24e7a29bc73d
	golang.org/dl v0.0.0-20230818220345-55c644201171
	golang.org/x/sys v0.10.0
)

replace (
	github.com/pawelgaczynski/giouring => ../giouring
)
