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
		String retorno = "";
		ArrayList<String> campos = nombreCampos(table);
		String fieldList = getFieldList(campos);
		retorno = "INSERT INTO " + table + " SELECT " + fieldList + " FROM " + table + " WHERE " + filter;
		return(retorno);
	}

	private String getFieldList(ArrayList<String> campos) {
		String retorno = "";
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
			retorno = retorno + campo + ",";
		}
		retorno = retorno.substring(0, retorno.length() - 1);
		return retorno;
	}
	
	private String processField(String field) {
		String retorno = field;
		if (this.values.containsKey(field)) {
			Object value = this.values.get(field);
			if (value instanceof String) {
				retorno = "'" + (String)value + "'";
			} else if (value instanceof Integer) {
				retorno = String.valueOf(value);
			} else {
				retorno = (String)value;
			}				
		}		
		return(retorno);
	}

	private ArrayList<String> nombreCampos(String table) {
		ArrayList<String> retorno = new ArrayList<String>();
		try {
			Statement st = connection.createStatement();
			ResultSet resultSet = st.executeQuery("SELECT top 1 * FROM " + table);
			ResultSetMetaData metadata = resultSet.getMetaData();
		    int columnCount = metadata.getColumnCount();
		    for (int i = 1; i <= columnCount; i++) {
		      String columnName = metadata.getColumnName(i);
		      retorno.add(columnName);
		    }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return retorno;
	}

	public void resetear() {
		values.clear();		
	}
	
	
}
