# spark
语法文件及语法解析
- PaimonSqlExtensions.g4 
- PaimonSparkSqlExtensionsParser 
- PaimonSqlExtensionsAstBuilder

规则注入
- PaimonSparkSessionExtensions

逻辑 command 到物理 exec 的转换
- PaimonStrategy

# Flink
```text
// createDynamicTableSource 入口， BaseDataTableSource#getScanRuntimeProvider 构建
// createDynamicTableSink 入口， FlinkTableSinkBase#getSinkRuntimeProvider 构建
FlinkTableFactory
- createDynamicTableSource => getScanRuntimeProvider
- createDynamicTableSink => getSinkRuntimeProvider

// flink 1.18 procedure 测试类
ProcedurePositionalArgumentsITCase
```

- RowKind: INSERT("+I", (byte) 0), UPDATE_BEFORE("-U", (byte) 1), UPDATE_AFTER("+U", (byte) 2), DELETE("-D", (byte) 3); 
- FindKind: ADD((byte) 0), DELETE((byte) 1);
- FileSource: APPEND((byte) 0), COMPACT((byte) 1);