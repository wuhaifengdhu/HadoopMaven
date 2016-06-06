package com.haifwu.util;

public final class Constants {

    // avoid new
    private Constants() {
    }

    public static final String PIPESTR = "|";

    public static final String INPUT_PATH = "input.path";

    public static final String OUTPUT_PATH = "output.path";

    public static final String TABLE_NAME = "table.name";

    public static final String DEFAULT_TABLE_NAME = "SC_IRAS_BREFCP";

    public static final String HBASE_BATCH_SIZE = "hbase.batch.size";

    public static final int DEFAULT_HBASE_BATCH_SIZE = 5000;

    public static final String MAPRED_MAX_INPUT_SPLIT_SIZE = "mapred.max.input.split.size";

    public static final String MAPRED_TEXTOUTPUTFORMAT_SEPARATOR = "mapred.textoutputformat.separator";

    public static final String MAPRED_TASK_PARTITION = "mapred.task.partition";

    public static final String MAPRED_JOB_ID = "mapred.job.id";

    public static final String MAPRED_TASK_ID = "mapred.task.id";

    public static final String IO_SORT_MB = "io.sort.mb";

    public static final String MAPRED_TASK_TIMEOUT = "mapred.task.timeout";

    public static final String MAPRED_MAP_MAX_ATTEMPTS = "mapred.map.max.attempts";

    public static final String MAPRED_REDUCE_TASKS_SPECULATIVE_EXECUTION = "mapred.reduce.tasks.speculative.execution";

    public static final String MAPRED_MAP_TASKS_SPECULATIVE_EXECUTION = "mapred.map.tasks.speculative.execution";

    public static final String MAPREDUCE_JOB_MAX_SPLIT_LOCATIONS = "mapreduce.job.max.split.locations";

    public static final String MAPRED_MAX_SPLIT_SIZE = "mapred.max.split.size";

    public static final String MAPRED_MIN_SPLIT_SIZE = "mapred.min.split.size";

    public static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

    public static final String DEFAULT_ACTIVITY_ID_INDEX_TABLE_NAME = "SC_IRAS_BREFCP_ACTIVITY_ID_INDEX";
    
    public static final String DEFAULT_ACTIVITY_ID_TO_UUID_TABLE_NAME = "SC_IRAS_BREFCP_ACID_TO_UUID";
    
    public static final String COMBINE_CURRENT_SPLIT_INDEX ="COMBINE_CURRENT_SPLIT_INDEX";
    
    public static final String SIMULATION_DATA_NAME = "sim.date.name";
    
    public static final String COLUMN_PIPESTR = "_";
    
    public static final String REFERENCE_PIPESTR = "=";
    
    public static final String MAX_THREADPOOL_SIZE = "sim.thread.pool.size";
    
    public static final String HDFS_FILEPATH_PREFIX = "sim.hdfs.file.path.prefix";
    
    public static final String DEFAULT_HDFS_FILEPATH_PREFIX = "hdfs://horton/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/idiriskaccessserv/SC_BREFundingCheckpoint_BREFundingCheckpoint";
}
