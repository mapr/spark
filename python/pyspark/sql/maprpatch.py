from py4j.java_gateway import java_import, JavaObject

def mapr_session_patch(original_session, wrapped, gw, default_sample_size=1000.0, default_id_field ="_id"):

    # Import MapR DB Connector Java classes
    java_import(gw.jvm, "com.mapr.db.spark.sql.api.java.MapRDBJavaSession")

    mapr_j_session = gw.jvm.MapRDBJavaSession(original_session._jsparkSession)

    def loadFromMapRDB(table_name, schema = None, sample_size=default_sample_size):
        """
        Loads data from MapR-DB Table.

        :param table_name: MapR-DB table path.
        :param schema: schema representation.
        :param sample_size: sample size.
        :return: a DataFrame

        >>> spark.loadFromMapRDB("/test-table").collect()
        """
        df_reader = original_session.read \
            .format("com.mapr.db.spark.sql") \
            .option("tableName", table_name) \
            .option("sampleSize", sample_size)

        if schema:
            df_reader.schema(schema)

        return df_reader.load()

    original_session.loadFromMapRDB = loadFromMapRDB


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
