/*
 * Copyright (c) 2008 Plausible Labs Cooperative, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of any contributors
 *    may be used to endorse or promote products derived from this
 *    software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace mdb_sqlite.NET_Sample
{
        public class AccessExporter : IDisposable
    {
        private const string AccessConn = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0}";
        private const string SqliteConn = "Data Source={0};Version=3;";
        private OleDbConnection oleConn;
        private SQLiteConnection sqliteConn;

        /// <summary>
        /// Create a new exporter with the provided MS Access
        /// database
        /// </summary>
        /// <param name="srcMdbPath">A path to an Access database.</param>
        public AccessExporter(string srcMdbPath)
        {
            oleConn = GetAccessConn(srcMdbPath);  
            oleConn.Open();
        }

        public static OleDbConnection GetAccessConn(string path)
        {
            string strConn = String.Format(AccessConn, path);
            var conn  = new OleDbConnection(strConn);
            return conn;
        }

        /// <summary>
        /// Get table name collection from MS Access database.
        /// </summary>
        /// <returns></returns>
        private static List<string> GetTableNames(OleDbConnection accessConn)
        {
            List<string> tableNames = new List<string>();
            DataTable dt = null;

            // use restrictions to get user tables, not system tables
            // Schema Restrictions  :  http://msdn.microsoft.com/en-us/library/cc716722(v=vs.110).aspx
            string[] restrictions = new string[4] { null, null, null, "Table" };

            // get list of user tables
            dt = accessConn.GetSchema("Tables", restrictions);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                tableNames.Add(dt.Rows[i][2].ToString());
            }

            return tableNames;
        }

        private static List<string> GetTableNames(SQLiteConnection accessConn)
        {
            List<string> tableNames = new List<string>();
            DataTable dt = null;

            // use restrictions to get user tables, not system tables
            // Schema Restrictions  :  http://msdn.microsoft.com/en-us/library/cc716722(v=vs.110).aspx
            string[] restrictions = new string[4] { null, null, null, "Table" };

            // get list of user tables
            dt = accessConn.GetSchema("Tables", restrictions);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                tableNames.Add(dt.Rows[i][2].ToString());
            }

            return tableNames;
        }


        private static TableSchema GetTableSchema(OleDbConnection accessConn, string tblName)
        {
            DataTable dt_Columns = null;
            DataTable dt_Indexes = null;
            List<Column> _Columns = new List<Column>();
            List<Index> _Indexes = new List<Index>();
            List<Dictionary<string, object>> _Rows = new List<Dictionary<string, object>>();

            // get columns
            // Schema Restrictions  :  http://msdn.microsoft.com/en-us/library/cc716722(v=vs.110).aspx
            string[] restrictions = new string[4] { null, null, tblName, null };
            var table = accessConn.GetSchema("Columns", restrictions);
            var rows = table.AsEnumerable().ToList();
            // avoid [The source contains no DataRows] exception, check if there has any data.
            dt_Columns = rows.Any() ? rows.CopyToDataTable() : table.Clone();
            foreach (DataRow row in dt_Columns.Rows)
            {
                _Columns.Add(new Column { Name = row["COLUMN_NAME"].ToString(), DataType = TypeConversion((int)row["DATA_TYPE"]) });
            }
            dt_Columns.Dispose();

            // get indexes
            // can't find a way to get all indexes by table name using restrictions...
            // avoid [The source contains no DataRows] exception, check if there has any data.
            if (accessConn.GetSchema("Indexes").Select("TABLE_NAME = '" + tblName + "'").Any())
            {
                dt_Indexes = accessConn.GetSchema("Indexes").Select("TABLE_NAME = '" + tblName + "'").CopyToDataTable();
                foreach (DataRow row in dt_Indexes.Rows)
                {
                    if (!_Indexes.Any(x => x.Name == row["INDEX_NAME"].ToString()))
                    {   // not exists
                        _Indexes.Add(new Index { TableName = row["TABLE_NAME"].ToString(), Name = row["INDEX_NAME"].ToString(), isPrimaryKey = (bool)row["PRIMARY_KEY"], isUnique = (bool)row["UNIQUE"], IndexingFields = new List<string> { row["COLUMN_NAME"].ToString() } });
                    }
                    else
                    {   // exists
                        _Indexes.Single(x => x.Name == row["INDEX_NAME"].ToString()).IndexingFields.Add(row["COLUMN_NAME"].ToString());
                    }
                }
                dt_Indexes.Dispose();
            }

            return new TableSchema(tblName, _Columns, _Indexes);
        }

        /// <summary>
        /// Initialize Table object from MS Access database by table name.
        /// </summary>
        /// <param name="accessConn"></param>
        /// <param name="tblName">A Table's name</param>
        /// <param name="whereClause">过滤表达式</param>
        /// <returns></returns>
        private static Table GetTable(OleDbConnection accessConn, string tblName, string whereClause = "")
        {
            var schema = GetTableSchema(accessConn, tblName);

            // get data records
            // enclosing the reserved word in square brackets. ( ex : [...] )
            // http://blog.csdn.net/quanelaine/article/details/5905260
            if (!string.IsNullOrEmpty(whereClause))
                whereClause = " WHERE " + whereClause;
            var _Rows = GetRows(accessConn, tblName, whereClause);
            return new Table(schema, _Rows);
        }

        private static IEnumerable<Dictionary<string,object>> GetRows(OleDbConnection accessConn, string tblName, string whereClause)
        {
            if (!string.IsNullOrEmpty(whereClause))
                whereClause = " WHERE " + whereClause;
            using (OleDbCommand cmd = new OleDbCommand(String.Format("SELECT * FROM [{0}] {1}; ", tblName, whereClause), accessConn))
            {
                using (OleDbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Dictionary<string, object> row = new Dictionary<string, object>();

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row.Add(reader.GetName(i), reader.GetValue(i));
                        }
                        //_Rows.Add(row);
                        yield return row;
                    }
                }
            }
        }

        /// <summary>
        /// Create an SQLite table for the corresponding MS Access table.
        /// </summary>
        /// <param name="tbl">MS Access table</param>
        private static void CreateTable(SQLiteConnection sqliteDbConn, TableSchema tbl)
        {
            List<Column> columns = tbl.Columns;
            StringBuilder sb = new StringBuilder();

            /* Create the statement */
            sb.Append("CREATE TABLE " + escapeIdentifier(tbl.Name) + " (");

            int columnCount = columns.Count;
            for (int i = 0; i < columnCount; i++)
            {
                Column column = columns[i];

                sb.Append(escapeIdentifier(column.Name));
                sb.Append(" ");
                switch (column.DataType)
                {
                    /* Blob */
                    case "BINARY":
                    case "LONGBINARY":
                    case "OLE":
                        sb.Append("BLOB");
                        break;

                    /* Integers */
                    case "YESNO":
                    case "BOOLEAN":
                    case "BYTE":
                    case "INT":
                    case "LONG":
                    case "SHORT":
                        sb.Append("INTEGER");
                        break;

                    /* Timestamp */
                    case "SHORT_DATE_TIME":
                    case "DATETIME":
                    case "DATE":
                        sb.Append("DATETIME");
                        break;

                    /* Floating point */
                    case "DOUBLE":
                    case "FLOAT":
                    case "NUMERIC":
                    case "SINGLE":
                        sb.Append("DOUBLE");
                        break;
                        
                    case "DECIMAL":
                        sb.Append("DOUBLE");
                        break;

                    /* Strings */
                    case "TEXT":
                    case "GUID":
                    case "LONGTEXT":
                    case "MEMO":
                        sb.Append("TEXT");
                        break;

                    /* Money -- This can't be floating point, so let's be safe with strings */
                    case "MONEY":
                    case "CURRENCY":
                        sb.Append("TEXT");
                        break;

                    default:
                        throw new ArgumentException("Unhandled MS Access datatype:  " + column.DataType);
                }

                if (i + 1 < columnCount)
                    sb.Append(", ");
            }
            sb.Append(")");

            /* Start a transaction */
            using (SQLiteTransaction trans = sqliteDbConn.BeginTransaction())
            {
                using (SQLiteCommand cmd = new SQLiteCommand(sb.ToString(), sqliteDbConn))
                {
                    /* Execute it */
                    cmd.ExecuteNonQuery();
                }
                trans.Commit();
            }
        }

        /// <summary>
        /// Create an index in an SQLite table for the corresponding
        /// index in MS Access
        /// </summary>
        /// <param name="index">MS Access table's index collection</param>
        private static void CreateIndex(SQLiteConnection sqliteDbConn, Index index)
        {
            List<string> columns = index.IndexingFields;
            StringBuilder sb = new StringBuilder();

            /* Create the statement */
            string tableName = index.TableName;
            string indexName = tableName + "_" + index.Name;
            string uniqueString = index.isUnique ? "UNIQUE" : "";

            sb.Append("CREATE " + uniqueString + " INDEX " + escapeIdentifier(indexName));
            sb.Append(" ON " + escapeIdentifier(tableName) + " (");

            int columnCount = columns.Count;
            for (int i = 0; i < columnCount; i++)
            {
                string column = columns[i];

                sb.Append(escapeIdentifier(column));
                sb.Append(" ");
                if (i + 1 < columnCount)
                    sb.Append(", ");
            }
            sb.Append(")");

            /* Start a transaction */
            using (SQLiteTransaction trans = sqliteDbConn.BeginTransaction())
            {
                using (SQLiteCommand cmd = new SQLiteCommand(sb.ToString(),sqliteDbConn))
                {
                    /* Execute it */
                    cmd.ExecuteNonQuery();
                }
                trans.Commit();
            }
        }

        /// <summary>
        /// Iterate over and create SQLite tables for every table defined
        /// in the MS Access database.
        /// </summary>
        private static void CreateTables(OleDbConnection accessConn, SQLiteConnection sqliteDbConn, List<string > createTableNames = null)
        {
            if(createTableNames != null)
            {
                ValidTableNames(accessConn, createTableNames);
            }
            else
            {
                createTableNames = GetTableNames(accessConn);
            }
            foreach (string tblName in createTableNames)
            {
                TableSchema tableSchema = GetTableSchema(accessConn, tblName);
                CreateTable(sqliteDbConn, tableSchema);
                CreateIndexes(sqliteDbConn, tableSchema);
            }
        }

        private static void CreateTables(OleDbConnection accessConn, SQLiteConnection sqliteDbConn, Dictionary<string,string > createTableNameDict)
        {
            foreach (var item in createTableNameDict)
            {
                TableSchema tableSchema = GetTableSchema(accessConn, item.Key);
                AlterTableName(ref tableSchema, item.Value);
                CreateTable(sqliteDbConn, tableSchema);
                CreateIndexes(sqliteDbConn, tableSchema);
            }
        }

        private static void AlterTableName<T>(ref T table, string tableName) where T : TableSchema
        {
            table.Name = tableName;
            foreach(var item in table.Indexes)
            {
                item.TableName = tableName;
            }
        }

        /// <summary>
        /// 校验表格是否存在
        /// </summary>
        /// <param name="accessConn"></param>
        /// <param name="createTableNames"></param>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="Exception"></exception>
        private static void ValidTableNames(OleDbConnection accessConn, List<string> createTableNames, int interval = 1000)
        {
            List<string> tableNames = GetTableNames(accessConn);
            tableNames = tableNames.Select(c => c.ToUpper()).ToList();
            createTableNames = createTableNames .Select(c=> c.ToUpper()).ToList(); 
            if(createTableNames == null || createTableNames.Count == 0)
            {
                throw new ArgumentException("参数createTableNames不合法！");
            }
            string excepTableName = string.Empty;
            if( (excepTableName = createTableNames.Except(tableNames).FirstOrDefault()) != null)
            {
                throw new Exception($"表“{excepTableName}”不存在！");
            }
        } 


        /// <summary>
        /// Iterate over all data and populate the SQLite tables
        /// </summary>
        /// <param name="tbl">MS Access table</param>
        private static void PopulateTable(SQLiteConnection sqliteDbConn, Table tbl)
        {
            List<Column> columns = tbl.Columns;
            StringBuilder insertBuilder = new StringBuilder();
            StringBuilder valueBuilder = new StringBuilder();

            /* Record the column count */
            int columnCount = columns.Count;

            /* Build the INSERT statement (in two pieces simultaneously) */
            insertBuilder.Append("INSERT INTO " + escapeIdentifier(tbl.Name) + " (");
            valueBuilder.Append("(");

            for (int i = 0; i < columnCount; i++)
            {
                Column column = columns[i];

                /* The column name and the VALUE binding */
                insertBuilder.Append(escapeIdentifier(column.Name));
                valueBuilder.Append("@param" + (i + 1));

                if (i + 1 < columnCount)
                {
                    insertBuilder.Append(", ");
                    valueBuilder.Append(", ");
                }
            }

            /* Now append the VALUES piece */
            insertBuilder.Append(") VALUES ");
            insertBuilder.Append(valueBuilder);
            insertBuilder.Append(")");

            /* Start a transaction */
            using (SQLiteTransaction trans = sqliteDbConn.BeginTransaction())
            {
                using (SQLiteCommand cmd = new SQLiteCommand(insertBuilder.ToString(), sqliteDbConn))
                {
                    /* Kick off the insert spree */
                    foreach (var row in tbl.Rows)
                    {
                        /* Bind all the column values. We let JDBC do type conversion -- is this correct?. */
                        for (int i = 0; i < columnCount; i++)
                        {
                            Column column = columns[i];
                            Object value = row[column.Name];

                            /* If null, just bail out early and avoid a lot of NULL checking */
                            if (value == System.DBNull.Value || value == null)
                            {
                                cmd.Parameters.AddWithValue("@param" + (i + 1), value);
                                continue;
                            }

                            /* Perform any conversions */
                            switch (column.DataType)
                            {
                                case "BINARY":
                                case "LONGBINARY":
                                case "OLE":
                                    var formatter = new BinaryFormatter();
                                    using (var ms = new MemoryStream())
                                    {
                                        formatter.Serialize(ms, value);
                                        ms.Seek(0, SeekOrigin.Begin);
                                        cmd.Parameters.AddWithValue("@param" + (i + 1), ms.ToArray());
                                    }
                                    break;
                                case "FLOAT":
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), (Double)(float)value);
                                    break;
                                case "SINGLE":
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), (Double)(Single)value);
                                    break;
                                case "DOUBLE":
                                case "NUMERIC":
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), (Double)value);
                                    break;
                                case "MONEY":
                                case "CURRENCY":
                                case "DECIMAL":
                                    /* Store money as a string. Is there any other valid representation in SQLite? */
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), ((Decimal)value).ToString());
                                    break;
                                case "YESNO":
                                case "BOOLEAN":
                                    /* The SQLite JDBC driver does not handle boolean values */
                                    bool boolean;
                                    int intVal;

                                    /* Determine the value (1/0) */
                                    boolean = (bool)value;
                                    intVal = boolean ? 1 : 0;

                                    /* Store it */
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), intVal);
                                    break;
                                case "BYTE":
                                    // unboxing problem : 
                                    // http://blogs.msdn.com/b/ericlippert/archive/2009/03/19/representation-and-identity.aspx
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), (int)(byte)value);
                                    break;
                                case "INT":
                                case "LONG":
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), (int)value);
                                    break;
                                case "SHORT":
                                    // unboxing problem : 
                                    // http://blogs.msdn.com/b/ericlippert/archive/2009/03/19/representation-and-identity.aspx
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), (int)(short)value);
                                    break;
                                case "TEXT":
                                case "GUID":
                                case "LONGTEXT":
                                case "MEMO":
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), value.ToString());
                                    break;
                                default:
                                    cmd.Parameters.AddWithValue("@param" + (i + 1), value);
                                    break;
                            }

                        }
                        /* Execute it */
                        int retval = cmd.ExecuteNonQuery();
                        if (retval != 1)
                        {   // ERROR!
                            throw new SQLiteException("Insert Data Execution Error:  TableName - " + tbl.Name + "  |  Row - " + string.Join(", ", row.Select(x => "[" + x.Key + " | " + x.Value + "]").ToArray()));
                        }
                    }
                }
                trans.Commit();
            }

        }

        /// <summary>
        /// Iterate over all data and populate the SQLite tables
        /// </summary>
        private static void PopulateTables(OleDbConnection accessConn, SQLiteConnection sqliteDbConn) 
        {
            List<string> tableNames = GetTableNames(accessConn);

            foreach (string tblName in tableNames)
            {
                Table tbl = GetTable(accessConn, tblName);
                PopulateTable(sqliteDbConn, tbl);
            }
        }

        private static void PopulateTables(OleDbConnection accessConn, SQLiteConnection sqliteDbConn, Dictionary<string, string> createTableNameDict, string[] whereClauses = null)
        {
            int index = 0; 
            foreach (var item  in createTableNameDict)
            {
                Table tbl = GetTable(accessConn, item.Key, whereClauses[index++]);
                AlterTableName(ref tbl, item.Value);
                PopulateTable(sqliteDbConn, tbl);
            }
        }

        private static void PopulateTables(OleDbConnection accessConn, SQLiteConnection sqliteDbConn, List<string> createTableNames,  string[] whereClauses = null)
        {
            int index = 0;
            foreach (var item in createTableNames)
            {
                Table tbl = GetTable(accessConn, item, whereClauses?[index++]);
                PopulateTable(sqliteDbConn, tbl);
            }
        }

        /// <summary>
        /// Iterate over and create SQLite indeces for every index defined
        /// in the MS Access table.
        /// </summary>
        /// <param name="tbl">MS Access table</param>
        private static void CreateIndexes(SQLiteConnection sqliteDbConn, TableSchema tbl) 
        {
            List<Index> indexes = tbl.Indexes;

            foreach (Index index in indexes) 
            {
                CreateIndex(sqliteDbConn,index);
            }
        }

        /// <summary>
        /// Convert column's data type from MS Access database
        /// into corresponding text.
        /// Lookup table : 
        /// http://allenbrowne.com/ser-49.html
        /// http://msdn.microsoft.com/en-us/library/windows/desktop/ms675318(v=vs.85).aspx
        /// </summary>
        /// <param name="type">The type code from MS Access database.</param>
        /// <returns></returns>
        private static string TypeConversion(int type) 
        {
            // decimal / hex 對照表 for DATA_TYPE：  http://allenbrowne.com/ser-49.html
            // http://msdn.microsoft.com/en-us/library/windows/desktop/ms675318(v=vs.85).aspx
            // http://msdn.microsoft.com/en-us/library/windows/desktop/ms677495(v=vs.85).aspx
            switch (type)
            {   // use ADOX enumerated constants.
                case 130:
                case 202:
                    return "TEXT";
                case 203:
                    return "MEMO";
                case 17:
                    return "BYTE";
                case 2:
                    return "SHORT";
                case 3:
                    return "LONG";
                case 4:
                    return "SINGLE";
                case 5:
                    return "DOUBLE";
                case 6:
                    return "CURRENCY";
                case 7:
                    return "DATETIME";
                case 11:
                    return "YESNO";
                case 72:
                    return "GUID";
                case 14:
                case 131:
                case 139:
                    return "DECIMAL";
                case 128:
                case 204:
                    return "BINARY";
                case 205:
                    return "LONGBINARY";
                default:
                    throw new ArgumentException("Unhandled MS Access datatype:  " + type);
            }
        }

        /// <summary>
        /// Export the Access database to SQLite.
        /// The referenced SQLite database should be empty.
        /// </summary>
        /// <param name="exportDest">The path to create a new SQLite database.</param>
        public void export(string exportDest)
        {
            /* Create the database */
            SQLiteConnection.CreateFile(exportDest);
            sqliteConn = new SQLiteConnection(String.Format(SqliteConn, exportDest));
            sqliteConn.Open();
            
            /* Create the tables */
            CreateTables(oleConn, sqliteConn);

            /* Populate the tables */
            PopulateTables(oleConn, sqliteConn);
        }

        /// <summary>
        /// mdb导出表格到sqlite，若表格存在则默认覆盖
        /// </summary>
        /// <param name="mdbPath"></param>
        /// <param name="sqlitePath"></param>
        /// <param name="ignoreExit">true: 若导入的表格已经存在，则不导入；false: 覆盖</param>
        /// <param name="tableNames">为空时，默认添加access数据库中的所有表</param>
        /// <exception cref="Exception"></exception>
        public static void AppendTables(string mdbPath, string sqlitePath, bool ignoreExistTable = false, params string[] tableNames)
        {
            if (!System.IO.File.Exists(mdbPath))
                throw new Exception($"文件“{mdbPath}”不存在！");
            if(!System.IO.File.Exists(sqlitePath))
                SQLiteConnection.CreateFile(sqlitePath);
            var sqliteDbConn = new SQLiteConnection(String.Format(SqliteConn, sqlitePath));
            sqliteDbConn.Open();
            var accessConn = GetAccessConn(mdbPath);
            accessConn.Open();
            var tableNameList = tableNames.ToList();
            if(tableNames is null || tableNames.Length == 0 )
                tableNameList = GetTableNames(accessConn);

            var sqliteTableNames = GetTableNames(sqliteDbConn);

            for (int i = tableNameList.Count - 1; i >= 0; i--)
            {//移除已经存在的表名
                if (sqliteTableNames.Any(c => c.Equals(tableNameList[i], StringComparison.OrdinalIgnoreCase)))
                {
                    if (ignoreExistTable)
                        tableNameList.RemoveAt(i);
                    else
                    {
                        if (!DropSqliteTableIfExsit(sqliteDbConn, tableNameList[i]))
                            throw new Exception($"删除表“{tableNameList[i]}”失败！");
                    }
                }
            }

            if (tableNameList.Count > 0)
            {
                ValidTableNames(accessConn, tableNameList);
                /* Create the tables */
                CreateTables(accessConn, sqliteDbConn, tableNameList);
                /* Populate the tables */
                PopulateTables(accessConn, sqliteDbConn, tableNameList);
            }
            Dispose(accessConn, sqliteDbConn);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mdbPath"></param>
        /// <param name="sqlitePath"></param>
        /// <param name="ignoreExistTable"></param>
        /// <param name="tableNameMappers">value为导出表格名称</param>
        /// <param name="exportAllTable">导出所有表，并且根据tableNameMappers中的映射表修改名称</param>
        /// <exception cref="Exception"></exception>
        public static void AppendTables(string mdbPath, string sqlitePath, Dictionary<string, string> tableNameMappers, List<string> whereClauses = null, bool ignoreExistTable = false, bool exportAllTable = false)
        {
            OleDbConnection accessConn = null;
            SQLiteConnection sqliteDbConn = null; ;
            try
            {
                if (!System.IO.File.Exists(mdbPath))
                    throw new Exception($"文件“{mdbPath}”不存在！");
                if (!System.IO.File.Exists(sqlitePath))
                    SQLiteConnection.CreateFile(sqlitePath);
                sqliteDbConn = new SQLiteConnection(String.Format(SqliteConn, sqlitePath));
                sqliteDbConn.Open();
                 accessConn = GetAccessConn(mdbPath);
                accessConn.Open();
                if (tableNameMappers == null || tableNameMappers.Count == 0 )
                {
                throw new ArgumentException("tableNameMappers不可为空！");
                }
                if (exportAllTable)
                {
                    var tableNameList = GetTableNames(accessConn);
                    foreach (var tableName in tableNameList)
                    {
                        if (!tableNameMappers.Any(c => c.Key.Equals(tableName, StringComparison.OrdinalIgnoreCase) 
                            || c.Value.Equals(tableName, StringComparison.Ordinal)))
                        {
                            tableNameMappers.Add(tableName, tableName);
                            whereClauses.Add("");
                        }
                    }
                }

                var sqliteTableNames = GetTableNames(sqliteDbConn);

                foreach (var tableName in tableNameMappers.Values)
                //for (int i = tableNameList.Count - 1; i >= 0; i--)
                {//移除已经存在的表名
                    if (sqliteTableNames.Any(c => c.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
                    {
                        if (ignoreExistTable)
                            tableNameMappers.Remove(tableName);
                        else
                        {
                            if (!DropSqliteTableIfExsit(sqliteDbConn, tableName))
                                throw new Exception($"删除表“{tableName}”失败！");
                        }
                    }
                }

                if (tableNameMappers.Count > 0)
                {
                    ValidTableNames(accessConn, tableNameMappers.Keys.ToList());
                    /* Create the tables */
                    CreateTables(accessConn, sqliteDbConn, tableNameMappers);
                    /* Populate the tables */
                    PopulateTables(accessConn, sqliteDbConn, tableNameMappers, whereClauses.ToArray());
                }
            }
            catch ( Exception ex)
            {
                throw;
            }
            finally
            {
                Dispose(accessConn, sqliteDbConn);
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private static bool DropSqliteTableIfExsit(SQLiteConnection sqliteDbConn, string tableName)
        {
            try
            {
                SQLiteCommand cmd = new SQLiteCommand();
                cmd.Connection = sqliteDbConn;
                cmd.CommandText = $"DROP TABLE IF EXISTS {tableName}";
                cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {
                return false; 
            }
            return true;
        }


        /// <summary>
        /// Dispose & close all connections.
        /// </summary>
        public void Dispose()
        {
            Dispose(oleConn,sqliteConn);
        }

        private static void Dispose(OleDbConnection conn, SQLiteConnection conn2)
        {
            conn?.Close();
            conn?.Dispose();
            conn2?.Close();
            conn2?.Dispose();
        }

        /* XXX: Manual escaping of identifiers. */
        private static String escapeIdentifier(String identifier)
        {
            return "'" + identifier.Replace("'", "''") + "'";
        }

        public class TableSchema
        {
            public TableSchema(string tblName, List<Column> _Columns, List<Index> _Indexes)
            {
                Name = tblName;
                Columns = _Columns;
                Indexes = _Indexes;
            }

            public string Name { get; set; }
            public List<Column> Columns { get; set; }
            public List<Index> Indexes { get; set; }
        }


        public class Table : TableSchema
        {
            public Table(TableSchema schema, IEnumerable<Dictionary<string, object>> _Rows) : base(schema.Name, schema.Columns, schema.Indexes)
            {
                Rows = _Rows;
            }

            public Table(string tblName, List<Column> _Columns, List<Index> _Indexes, IEnumerable<Dictionary<string, object>> _Rows) : base(tblName, _Columns, _Indexes)
            {
                //Name = tblName;
                //Columns = _Columns;
                //Indexes = _Indexes;
                Rows = _Rows;
            }
            //public string Name { get; set; }
            //public List<Column> Columns { get; set; }
            //public List<Index> Indexes { get; set; }
            public IEnumerable<Dictionary<string, object>> Rows { get; set; }
        }

        public class Column
        {
            public string Name { get; set; }
            public string DataType { get; set; }
        }

        public class Index
        {
            public string TableName { get; set; }
            public string Name { get; set; }
            public bool isPrimaryKey { get; set; }
            public bool isUnique { get; set; }
            public List<string> IndexingFields { get; set; }
        }
    }

}
