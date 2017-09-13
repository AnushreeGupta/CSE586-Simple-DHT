package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;

/**
 * Created by anushree on 4/13/17.
 *
 * Source Used - https://developer.android.com/training/basics/data-storage/databases.html
 */

public class SQLHelper extends SQLiteOpenHelper {

    /* Inner class that defines the table contents */
    public static final String DATABASE_NAME = "SimpleDHT";
    public static final String TABLE_NAME = "DataItems";
    public static final String COLUMN_1 = "key";
    public static final String COLUMN_2 = "value";

    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + SQLHelper.TABLE_NAME + " (" +
                    SQLHelper.COLUMN_1 + " TEXT PRIMARY KEY," +
                    SQLHelper.COLUMN_2 + " TEXT NOT NULL, UNIQUE (key) ON CONFLICT REPLACE);";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + SQLHelper.TABLE_NAME;

    public SQLHelper(Context context){
        super(context,DATABASE_NAME,null,1);
    }
    @Override
    public void onCreate(SQLiteDatabase Db){
        Db.execSQL(SQL_CREATE_ENTRIES);
    }

    @Override
    public void onUpgrade(SQLiteDatabase Db,int oldV, int newV){

    }

    public void onDelete(SQLiteDatabase Db) {
        Db.execSQL(SQL_DELETE_ENTRIES);

    }

    public ContentValues updateContentValue( ContentValues cValues, String key, String value){
        cValues.put("key", key);
        cValues.put("value", value);
        return cValues;
    }

}
