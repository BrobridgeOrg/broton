package broton

import "github.com/tecbot/gorocksdb"

type Options struct {
	DatabasePath   string
	RocksdbOptions *gorocksdb.Options
}

func NewOptions() *Options {

	// Well, I am not really sure what i am writing right here. hope it won't get any troubles. :-S
	options := gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetEnablePipelinedWrite(true)
	options.SetAllowConcurrentMemtableWrites(true)
	options.SetOptimizeFiltersForHits(true)
	options.SetNumLevels(4)

	blockBasedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	blockBasedTableOptions.SetBlockSizeDeviation(5)
	blockBasedTableOptions.SetBlockSize(4 * 1024)
	blockBasedTableOptions.SetCacheIndexAndFilterBlocks(true)
	blockBasedTableOptions.SetCacheIndexAndFilterBlocksWithHighPriority(true)
	blockBasedTableOptions.SetPinL0FilterAndIndexBlocksInCache(true)
	//blockBasedTableOptions.SetIndexType(gorocksdb.KHashSearchIndexType)
	options.SetBlockBasedTableFactory(blockBasedTableOptions)

	env := gorocksdb.NewDefaultEnv()
	env.SetBackgroundThreads(4)
	options.SetMaxBackgroundCompactions(4)
	options.SetTargetFileSizeBase(64 * 1024 * 1024)
	options.SetMaxWriteBufferNumber(3)
	options.SetLevel0FileNumCompactionTrigger(8)
	options.SetLevel0SlowdownWritesTrigger(17)
	options.SetLevel0StopWritesTrigger(24)
	options.SetMaxBytesForLevelBase(512 * 1024 * 1024)
	options.SetMaxBytesForLevelMultiplier(8)
	options.SetMaxOpenFiles(-1)
	options.SetEnv(env)

	return &Options{
		RocksdbOptions: options,
	}
}
