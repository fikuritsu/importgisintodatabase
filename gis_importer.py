##############################
# geofile_importer.py
#
# about the code:
# to import gpx,kml,shape files into PostgreSQL
## Created on: 12 Nov 2020
## by: symfikri@gmail.com
## edited on:19 Nov 2020
## reason: edit the gpx function
#################################

import os
import subprocess
import gdssDatabase
from gdssDatabase import Database


class geoImporter(object):
    #directory of the geofile
     def __init__(self, directory):
        self.directory=directory
        # self.directory='/var/www/cgi-bin/20201014065959-report.kml'
        self.filename = os.path.basename(directory)
        filename2 = os.path.splitext(self.filename)[0]
        file_type = os.path.splitext(self.filename)[1]

        #the layer of the gpx that you want to import
        tablename="waypoints"

        #sql commands definition
        FETCH_ALL_COLUMNS=f"""
        SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name ='{tablename}';
        """


    ##########################################################################################################################################################
    ###                        The definition of functions that are being used in this code
    ##########################################################################################################################################################
        def fetch_ID(name):
            FETCH_ID_GROUP = f"""
            SELECT id, group_id FROM inventory WHERE filename = '{name}';
            """
            return FETCH_ID_GROUP

        def get_filename(name):
            query=fetch_ID(name)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchone()

        def sql_remove_tracks(column):
            REMOVE_TRACKS = f"""
            DELETE FROM "{column}" WHERE ogc_fid='1';
            """
            return REMOVE_TRACKS

        def remove_tracks(column):
            query=sql_remove_tracks(column)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)


        def get_columns():
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(FETCH_ALL_COLUMNS)
                    return cursor.fetchall()


        def sql_column(clean):
            SELECT_COLUMNS = f"""
            SELECT * FROM {tablename} WHERE {clean} IS NULL;
            """
            return SELECT_COLUMNS


        def sql_delete(clean):
            DELETE_COLUMNS = f"""
            ALTER TABLE {tablename} DROP {clean};
            """
            return DELETE_COLUMNS

        def select_columns(clean):
            query1=sql_column(clean)
            # print(query1)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query1)
                    return  cursor.rowcount

        def delete_columns(clean):
            query2 = sql_delete(clean)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query2)


        def clear_null():
            lists = get_columns()

            cleaned = ['"' + i[0] + '"' for i in lists]

            for clean in cleaned:
                # print(clean)
                # select_columns(clean)
                logic = select_columns(clean)
                # print(logic)
                if logic != 0:
                    delete_columns(clean)
                    # print("the column is deleted")
                        # delete_columns()
                # else:
                    # print("the column is saved")

        def sql_rename_tables(attr_table_name):
            RENAME_TABLES = f"""
            ALTER TABLE "{tablename}"
            RENAME TO "{attr_table_name}";
            """
            return RENAME_TABLES

        def rename_tables(attr_table_name):
            query=sql_rename_tables(attr_table_name)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)


################################################################################################################################################################
#    The code below is import the geo files (gpx,geojson,kml and shape file)
################################################################################################################################################################
        def sql_check_tablename(filename2):
            CHECK_TABLE = f"""
            SELECT EXISTS (SELECT table_name FROM information_schema.tables WHERE table_name = '{filename2}');
            """
            return CHECK_TABLE

        def check_tablename(filename2):
            query=sql_check_tablename(filename2.lower())
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchone()

        def sql_error1(filename2):
            ERROR_HANDER1 = f"""
            DROP TABLE IF EXISTS "{filename2}";
            """
            return ERROR_HANDER1

        def sql_error2():
            ERROR_HANDER2 = """
            DROP TABLE IF EXISTS waypoints;
            """
            return ERROR_HANDER2

        def sql_geom(filename2):
            REMOVE_GEOM=f"""
            ALTER table "{filename2}" DROP IF EXISTS "geom"
            ALTER table "{filename2}" DROP IF EXISTS "wkb_geometry"
            """
            return REMOVE_GEOM

        def remove_geom(filename2):
            query = sql_geom(filename2)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)

        def sql_gpx_change_seq(attr_table):
            CHANGE_WPOINTS=f"""
            ALTER SEQUENCE "waypoints_ogc_fid_seq" RENAME TO "{attr_table}_ogc_fid_seq"
            """
            return CHANGE_WPOINTS

        def gpx_change_seq(attr_table):
            query=sql_gpx_change_seq(attr_table)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)

        def sql_gpx_change_index(attr_table):
            CHANGE_INDEX=f"""
            ALTER INDEX "waypoints_pkey" RENAME TO "{attr_table}_pkey";
            ALTER INDEX "waypoints_wkb_geometry_geom_idx" RENAME TO "{attr_table}_wkb_geometry_geom_idx";
            """
            return CHANGE_INDEX

        def gpx_change_index(attr_table):
            query=sql_gpx_change_index(attr_table)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)

        def error_handling(filename2):
            query=sql_error1(filename2)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)

        def error_handling2():
            query=sql_error2()
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)

        #the function to get/read the gpx file
        def get_gpx_file(directory, filename2):
            #print(filename2)
            try:
                name = f"{filename2}.gpx"
                c = get_filename(name)
                attr_table_name = (f"att_{c[0]}_{c[1]}")
                p = check_tablename(attr_table_name)
                #print(attr_table_name)
                # check = [i[0] for i in p]
                # print(check)
                check = p[0]
                # print(check)
                if (check == True):
                    print("The file is existed in the database!")
                else:
                    try:
                        # cmd = f'ogr2ogr -f "PostgreSQL" PG:"host=localhost user=alvin dbname=gdssdb password=Passw0rd port=5432" {directory} {tablename}'
                        cmd = f'ogr2ogr -f "PostgreSQL" PG:"host=localhost user=postgres dbname=gdss password=5430 port=5432" {directory} {tablename}'
                        # subprocess.call(cmd, shell=True)
                        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
                        p.wait()
                        p.kill()
                        clear_null()
                        #remove_geom(attr_table_name)
                        rename_tables(attr_table_name)
                        gpx_change_seq(attr_table_name)
                        gpx_change_index(attr_table_name)
                    except BaseException as e:
                        print('Failed to do something: ' + str(e))
                        error_handling(attr_table_name)
                        error_handling2()
            except BaseException as e:
                print('Something went wrong! '+ str(e))

        #the function to import/read geoJSON file

        def importjson(directory, filename2):
            try:
                name = f"{filename2}.json"
                c = get_filename(name)
                attr_table_name = (f"att_{c[0]}_{c[1]}")
                p = check_tablename(attr_table_name)
                # check = [i[0] for i in p]
                # print(check)
                check = p[0]
                # print(check)
                if (check == True):
                    print("The file is existed in the database!")
                else:
                    try:
                        # cmd = f'ogr2ogr -f "PostgreSQL" PG:"host=localhost user=alvin dbname=gdssdb password=Passw0rd port=5432" {directory} -nln {attr_table_name}'
                        cmd = f'ogr2ogr -f "PostgreSQL" PG:"host=localhost user=postgres dbname=gdss password=5430 port=5432" {directory} -nln {attr_table_name}'
                        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
                        p.wait()
                        p.kill()
                        #remove_geom(attr_table_name)
                        remove_tracks(attr_table_name)
                    except BaseException as e:
                        print('Failed to do something: ' + str(e))
            except BaseException as e:
                # print('Failed to do something: ' + str(e))
                print('Something went wrong!')

        #the function to import/read the kml file
        def importkml(directory, filename2):
            try:
                name = f"{filename2}.kml"
                c = get_filename(name)
                attr_table_name = (f"att_{c[0]}_{c[1]}")
                p = check_tablename(attr_table_name)
                # check = [i[0] for i in p]
                # print(check)
                check = p[0]
                # print(check)
                if (check == True):
                    print("The file is existed in the database!")
                else:
                    try:
                        # cmd = f'ogr2ogr -f "PostgreSQL" PG:"host=localhost user=alvin dbname=gdssdb password=Passw0rd port=5432" {directory} -nln {attr_table_name}'
                        cmd = f'ogr2ogr -f "PostgreSQL" PG:"host=localhost user=postgres dbname=gdss password=5430 port=5432" {directory} -nln {attr_table_name}'
                        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
                        p.wait()
                        p.kill()
                        #remove_geom(attr_table_name)
                        remove_tracks(attr_table_name)
                    except BaseException as e:
                        # print('Failed to do something: ' + str(e))
                        print('Failed to do something:')
            except BaseException as e:
                # print('Failed to do something: ' + str(e))
                print('Something went wrong!')
        #

        #the function to import/read the shape file
        def importshp(directory, filename2):
            try:
                name = f"{filename2}.shp"
                c = get_filename(name)
                attr_table_name = (f"att_{c[0]}_{c[1]}")
                p = check_tablename(attr_table_name)
                # check = [i[0] for i in p]
                check = p[0]
                # print(check)
                if (check == True):
                    print("The file is existed in the database!")
                else:
                    try:
                        # print(attr_table_name)
                        # cmd = f'shp2pgsql -s 4326 -D -I {directory} {attr_table_name} | psql -h "localhost" -d "gdssdb" -U "alvin"'
                        cmd = f'shp2pgsql -s 4326 -D -I {directory} {attr_table_name} | psql -h "localhost" -d "gdss" -U "postgres"'
                        retcode = subprocess.call(cmd, shell=True,
                                                  stdout=subprocess.DEVNULL,
                                                  stderr=subprocess.STDOUT)
                        #remove_geom(attr_table_name)
                    except BaseException as e:
                        print('Failed to do something: ' + str(e))
            except BaseException as e:
                print('Something went wrong! ')

        if file_type==".gpx":
            get_gpx_file(directory, filename2)
        elif file_type == ".json":
            importjson(directory, filename2)
        elif file_type == ".kml":
            importkml(directory, filename2)
        elif file_type == ".shp":
            importshp(directory, filename2)
        else:
            print("Error! Please enter .shp, .kml, .geoJSON or .gpx files only!")

# try:
#     c=Database()
#     connection=c.connectDB()
#     #print(connection)
#
#
#     disconnect=c.disconnectDB
#     #print(disconnect)
# except BaseException as e:
#     print('Failed to do something: ' + str(e))
#     print('The source is wrong!')

data_source=input("Please enter the directory of source file(kml,gpx,shp):")

if(data_source):
    try:
        c=Database()
        connection=c.connectDB()
        #print(connection)
        with connection:
            foo=geoImporter(data_source)
            #print(foo.filename)

        disconnect=c.disconnectDB
        #print(disconnect)
    except BaseException as e:
        print('Failed to do something: ' + str(e))
        print('The source is wrong!')