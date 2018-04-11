package copymaker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.UnaryOperator;

public class CopyMaker {

	private Map<String, Object> values;
	private Connection connection;

	public CopyMaker(Connection conn) {
		super();
		this.connection = conn;
		this.values = new HashMap<String, Object>();
	}
	
	public CopyMaker addNumber(String field, Integer value) {
		values.put(field, value);
		return(this);
	}
	
	public CopyMaker addString(String field, String value) {
		values.put(field, value);
		return(this);
	}
	
	public String getSql(String table, String filter) {
		String returnValue = "";
		ArrayList<String> fields = fieldNames(table);
		String fieldList = getFieldList(fields);
		returnValue = "INSERT INTO " + table + " SELECT " + fieldList + " FROM " + table + " WHERE " + filter;
		return(returnValue);
	}

	private String getFieldList(ArrayList<String> campos) {
		String returnValue = "";
		UnaryOperator<String> x = new UnaryOperator<String>() {
			@Override
			public String apply(String y) {
				return processField(y);
			}
		};
		campos.replaceAll(x);
		Iterator<String> recorrerCampos = campos.iterator();
		while (recorrerCampos.hasNext()) {
			String campo = recorrerCampos.next();
			returnValue = returnValue + campo + ",";
		}
		returnValue = returnValue.substring(0, returnValue.length() - 1);
		return returnValue;
	}
	
	private String processField(String field) {
		String returnValue = field;
		if (this.values.containsKey(field)) {
			Object value = this.values.get(field);
			if (value instanceof String) {
				returnValue = "'" + (String)value + "'";
			} else if (value instanceof Integer) {
				returnValue = String.valueOf(value);
			} else {
				returnValue = (String)value;
			}				
		}		
		return(returnValue);
	}

	private ArrayList<String> fieldNames(String table) {
		ArrayList<String> returnValue = new ArrayList<String>();
		try {
			Statement st = connection.createStatement();
			ResultSet resultSet = st.executeQuery("SELECT top 1 * FROM " + table);
			ResultSetMetaData metadata = resultSet.getMetaData();
		    int columnCount = metadata.getColumnCount();
		    for (int i = 1; i <= columnCount; i++) {
		      String columnName = metadata.getColumnName(i);
		      returnValue.add(columnName);
		    }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return returnValue;
	}

	public void reset() {
		values.clear();		
	}
	
	
}
