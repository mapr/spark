from py4j.java_gateway import java_import, JavaObject
from pyspark.sql.dataframe import DataFrame

def mapr_session_patch(original_session, wrapped, gw, default_sample_size=1000.0, default_id_field ="_id", buffer_writes=True):

    vars = {'buffer_writes': buffer_writes, 'indexPath': None, 'options': {}}

    # Import MapR DB Connector Java classes
    java_import(gw.jvm, "com.mapr.db.spark.sql.api.java.MapRDBJavaSession")

    mapr_j_session = gw.jvm.MapRDBJavaSession(original_session._jsparkSession)

    def setBufferWrites(bw=vars['buffer_writes']):
        mapr_j_session.setBufferWrites(bw)
        vars['buffer_writes'] = bw
    original_session.setBufferWrites = setBufferWrites

    def setHintUsingIndex(indexPath):
        mapr_j_session.setHintUsingIndex(indexPath)
        vars['indexPath'] = indexPath
    original_session.setHintUsingIndex = setHintUsingIndex

    def setQueryOption(key, value):
        mapr_j_session.setQueryOption(key, value)
        vars['options'][key] = value
    original_session.setQueryOption = setQueryOption

    def setQueryOptions(options):
        mapr_j_session.setQueryOptions(options)
        vars['options'] = options
    original_session.setQueryOptions = setQueryOptions

    def loadFromMapRDB(table_name, schema = None, sample_size=default_sample_size):
        """
        Loads data from MapR-DB Table.

        :param buffer_writes: buffer-writes ojai parameter
        :param table_name: MapR-DB table path.
        :param schema: schema representation.
        :param sample_size: sample size.
        :return: a DataFrame

        >>> spark.loadFromMapRDB("/test-table").collect()
        """
        df_reader = original_session.read \
            .format("com.mapr.db.spark.sql") \
            .option("tableName", table_name) \
            .option("sampleSize", sample_size) \
            .option("bufferWrites", vars['buffer_writes']) \
            .option("indexPath", vars['indexPath']) \
            .options(**vars['options'])

        if schema:
            df_reader.schema(schema)

        return df_reader.load()

    original_session.loadFromMapRDB = loadFromMapRDB


    def lookupFromMapRDB(table_name, schema=None, sample_size=default_sample_size):

        options = dict(vars['options'])
        options['spark.maprdb.enforce_single_fragment'] = 'true'

        df_reader = original_session.read \
            .format("com.mapr.db.spark.sql") \
            .option("tableName", table_name) \
            .option("sampleSize", sample_size) \
            .option("bufferWrites", vars['buffer_writes']) \
            .option("indexPath", vars['indexPath']) \
            .options(**options)

        if schema:
            df_reader.schema(schema)

        return df_reader.load()

    original_session.lookupFromMapRDB = lookupFromMapRDB


    def saveToMapRDB(dataframe, table_name, id_field_path = default_id_field, create_table = False, bulk_insert = False):
        """
        Saves data to MapR-DB Table.

        :param dataframe: a DataFrame which will be saved.
        :param table_name: MapR-DB table path.
        :param id_field_path: field name of document ID.
        :param create_table: indicates if table creation required.
        :param bulk_insert: indicates bulk insert.
        :return: a RDD

        >>> spark.saveToMapRDB(df, "/test-table")
        """
        DataFrame(mapr_j_session.saveToMapRDB(dataframe._jdf, table_name, id_field_path, create_table, bulk_insert), wrapped)

    original_session.saveToMapRDB = saveToMapRDB


    def insertToMapRDB(dataframe, table_name, id_field_path = default_id_field, create_table = False, bulk_insert = False):
        """
        Inserts data into MapR-DB Table.

        :param dataframe: a DataFrame which will be saved.
        :param table_name: MapR-DB table path.
        :param id_field_path: field name of document ID.
        :param create_table: indicates if table creation required.
        :param bulk_insert: indicates bulk insert.
        :return: a RDD

        >>> spark.insertToMapRDB(df, "/test-table")
        """
        DataFrame(mapr_j_session.insertToMapRDB(dataframe._jdf, table_name, id_field_path, create_table, bulk_insert), wrapped)

    original_session.insertToMapRDB = insertToMapRDB
