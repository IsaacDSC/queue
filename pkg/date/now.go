package date

import "time"

var Now = func() time.Time {
	return time.Now()
}

func SetNow(n time.Time) {
	Now = func() time.Time {
		return n
	}
}
