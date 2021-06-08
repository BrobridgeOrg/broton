package broton

type Options struct {
	DatabasePath string
}

func NewOptions() *Options {

	return &Options{}
}
