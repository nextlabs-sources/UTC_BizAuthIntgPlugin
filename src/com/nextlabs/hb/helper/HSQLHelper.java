package com.nextlabs.hb.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.crypt.IDecryptor;
import com.bluejungle.framework.crypt.ReversibleEncryptor;

public class HSQLHelper {
	private static final Log LOG = LogFactory.getLog(HSQLHelper.class);
	String connectionUrl;
	String userName;
	String password;
	SimpleDateFormat dt;
	private IDecryptor decryptor = new ReversibleEncryptor();

	/*
	 * This constructor loads the driver and assigns the required connection
	 * variables
	 */
	public HSQLHelper(String connectionUrl, String userName, String password,
			String dateFormat) {
		this.connectionUrl = connectionUrl;
		this.userName = userName;
		this.password = decryptor.decrypt(password);
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
		} catch (ClassNotFoundException e) {
			LOG.error("HSQLHelper loading driver error: ", e);
		}
		dt = new SimpleDateFormat(dateFormat);
	}

	public HSQLHelper(String connectionUrl, String userName, String password) {
		this.connectionUrl = connectionUrl;
		this.userName = userName;
		this.password = decryptor.decrypt(password);
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
		} catch (ClassNotFoundException e) {
			LOG.error("HSQLHelper loading driver error: ", e);
		}
	}

	/* Establishes a connection with Hsql db */
	public Connection openConnection() {
		Connection hsqlConnection = null;
		try {
			hsqlConnection = DriverManager.getConnection(connectionUrl,
					userName, password);
			hsqlConnection.setAutoCommit(true);
			// LOG.info(" Connection Established with HSQL");
		} catch (SQLException e) {
			LOG.error("HSQLHelper connectToHsql() error: ", e);
			return null;
		}
		return hsqlConnection;
	}

	/* Closes a connection with hsqldb */
	private void closeConnection(Connection hsqlConnection) {
		try {
			if (!hsqlConnection.isClosed())
				hsqlConnection.close();
		} catch (SQLException e) {
			LOG.error("HSQLHelper closeConnection() error: ", e);
		}
	}

	/*
	 * This metod gets an attribute of a user based on the attribute given by
	 * the user
	 */
	public void insertDataWithDate(InsertQueryHelper iqh,
			List<HashMap<String, String>> data) {
		if (data != null && data.size() > 0) {
			Connection hsqlConnection = openConnection();
			HashMap<String, String> record = new HashMap<String, String>();
			if (hsqlConnection != null) {
				if (iqh.getQuery() != null) {
					try {
						LOG.info("query" + iqh.getQuery());
						PreparedStatement preparedStatement = hsqlConnection
								.prepareStatement(iqh.getQuery());
						ArrayList<String> fieldList = iqh.getFieldList();
						int rangeMax = 1000;
						List<HashMap<String, String>> subData;
						while (data.size() > 0) {
							if (data.size() > rangeMax) {
								subData = data.subList(0, rangeMax);
							} else {
								subData = data;
							}
							for (int i = 0; i < subData.size(); i++) {
								int fieldcount = 1;
								record = subData.get(i);
								if (record != null) {
									for (String field : fieldList) {
										if (field
												.equals(PluginConstants.COMMON_PROPS
														.getProperty("au_col_StartDate"))
												|| field.equals(PluginConstants.COMMON_PROPS
														.getProperty("au_col_EndDate"))
												|| field.equals(PluginConstants.COMMON_PROPS
														.getProperty("user_col_CGDEExpiration"))) {
											try {
												if (record.get(field) != null) {
													Date date = dt.parse(record
															.get(field)
															.toString());
													preparedStatement
															.setObject(
																	fieldcount,
																	date);
												} else {
													preparedStatement
															.setObject(
																	fieldcount,
																	null);
												}
											} catch (Exception e) {
												preparedStatement.setObject(
														fieldcount, null);
												LOG.warn(
														"HSQLHelper insertData() warn: in parsing data "
																+ record, e);
											}
										} else {
											if (record.get(field) != null) {
												/*
												 * LOG.info(field + ":" +
												 * record.get(field));
												 */
												preparedStatement.setObject(
														fieldcount,
														record.get(field));
											} else {
												preparedStatement.setObject(
														fieldcount, null);
											}
										}
										fieldcount++;
									}
								}
								// LOG.info("RECORD NO "+i+" : "+ record);
								if (subData.size() > 1) {
									preparedStatement.addBatch();
								}
							}
							if (subData.size() > 1) {
								preparedStatement.executeBatch();
								hsqlConnection.createStatement().executeUpdate(
										"COMMIT");
							} else if (subData.size() == 1) {
								preparedStatement.executeUpdate();
							}
							subData.clear();
						}
					} catch (SQLException e) {
						LOG.error("HSQLHelper SQL insertData() error: ", e);
						LOG.error(record);
					} catch (Exception e) {
						LOG.error("HSQLHelper insertData() error: ", e);
						LOG.error(record);
					}
				}
				closeConnection(hsqlConnection);
			}
		}
	}

	public void insertData(InsertQueryHelper iqh,
			List<HashMap<String, String>> data) {
		if (data != null && data.size() > 0) {
			Connection hsqlConnection = openConnection();
			if (hsqlConnection != null) {
				if (iqh.getQuery() != null) {
					try {
						PreparedStatement preparedStatement = hsqlConnection
								.prepareStatement(iqh.getQuery());
						ArrayList<String> fieldList = iqh.getFieldList();
						int rangeMax = 50;
						List<HashMap<String, String>> subData;
						while (data.size() > 0) {
							if (data.size() > rangeMax) {
								subData = data.subList(0, rangeMax);
							} else {
								subData = data;
							}
							for (int i = 0; i < subData.size(); i++) {
								int fieldcount = 1;
								HashMap<String, String> record = subData.get(i);
								if (record != null) {
									for (String field : fieldList) {
										// LOG.info(field + ":" +
										// record.get(field));
										if (record.get(field) != null) {
											preparedStatement.setObject(
													fieldcount,
													record.get(field));
										} else {
											preparedStatement.setObject(
													fieldcount, null);
										}
										fieldcount++;
									}
								}
								// LOG.info("RECORD NO "+i+" : "+ record);
								if (subData.size() > 1) {
									preparedStatement.addBatch();
								}
							}
							if (subData.size() > 1) {
								preparedStatement.executeBatch();
								hsqlConnection.createStatement().executeUpdate(
										"COMMIT");
							} else if (subData.size() == 1) {
								preparedStatement.executeUpdate();
							}
							subData.clear();
						}
					} catch (SQLException e) {
						LOG.error("HSQLHelper insertData() error: ", e);
						LOG.error("Tableerror Name:" + iqh.getTableName());
					} catch (Exception e) {
						LOG.error("HSQLHelper insertData() error: ", e);
					}
				}
				closeConnection(hsqlConnection);
			}
		}
	}

	public void truncatetables(InsertQueryHelper iqh) {
		Connection hsqlConnection = openConnection();
		if (hsqlConnection != null) {
			if (iqh.getQuery() != null) {
				try {
					ArrayList<String> fieldList = iqh.getFieldList();
					Statement st = hsqlConnection.createStatement();
					for (String field : fieldList) {
						st.execute("TRUNCATE TABLE " + field);
					}
				} catch (SQLException e) {
					LOG.error("HSQLHelper insertData() error: ", e);
				} catch (Exception e) {
					LOG.error("HSQLHelper insertData() error: ", e);
				}
			}
			closeConnection(hsqlConnection);
		}
	}

	public Date getTimeStamp() {
		Connection hsqlConnection = openConnection();
		Date result = null;
		String query = "SELECT * FROM TIMESTAMP";
		if (hsqlConnection != null) {
			try {
				Statement hsqlStatement = hsqlConnection.createStatement();
				ResultSet rs = hsqlStatement.executeQuery(query);
				while (rs.next()) {
					result = new Date(rs.getTimestamp(1).getTime());
				}
				LOG.info("Date" + result);
			} catch (SQLException e) {
				LOG.error("HSQLHelper getTimeStamp() error: ", e);
				result = null;
			}
			closeConnection(hsqlConnection);
		}
		return result;
	}

	public void updateTimeStamp() {
		Connection hsqlConnection = openConnection();
		String query = "UPDATE TIMESTAMP SET UPDATEDTIME=CURRENT_TIMESTAMP";
		if (hsqlConnection != null) {
			try {
				Statement hsqlStatement = hsqlConnection.createStatement();
				LOG.info("Record count" + hsqlStatement.executeUpdate(query));
			} catch (SQLException e) {
				LOG.error("HSQLHelper updatetime() error: ", e);
			}
			closeConnection(hsqlConnection);
		}
	}

	public void updateIsStale(String stale) {
		Connection hsqlConnection = openConnection();
		String query = "UPDATE TIMESTAMP SET ISSTALE=" + stale;
		if (hsqlConnection != null) {
			try {
				Statement hsqlStatement = hsqlConnection.createStatement();
				LOG.info("Record count" + hsqlStatement.executeUpdate(query));
			} catch (SQLException e) {
				LOG.error("HSQLHelper updateIsStale() error: ", e);
			}
			closeConnection(hsqlConnection);
		}
	}

	public void deleteContentsForCCLCountries(String query) {
		Connection hsqlConnection = openConnection();
		if (hsqlConnection != null) {
			try {
				Statement hsqlStatement = hsqlConnection.createStatement();
				hsqlStatement.executeUpdate(query);
			} catch (SQLException e) {
			}
			closeConnection(hsqlConnection);
		}
	}

	public HashMap<String, HashMap<String, Integer>> retrieveJurisdictionClassificationData() {
		Connection hsqlConnection = openConnection();
		HashMap<String, HashMap<String, Integer>> jurisClassCode = null;
		if (hsqlConnection != null) {
			try {
				String query = MessageFormat.format(PluginConstants.BASEQUERY,
						PluginConstants.COMMON_PROPS.getProperty("table_jcm"));
				Statement hsqlStatement = hsqlConnection.createStatement();
				ResultSet rs = hsqlStatement.executeQuery(query);
				jurisClassCode = new HashMap<String, HashMap<String, Integer>>();
				int count = 0;
				while (rs.next()) {
					if (jurisClassCode.get(rs.getString(2)) == null) {
						HashMap<String, Integer> map = new HashMap<String, Integer>();
						map.put(rs.getString(3), rs.getInt(1));
						jurisClassCode.put(rs.getString(2), map);
					} else {
						jurisClassCode.get(rs.getString(2)).put(
								rs.getString(3), rs.getInt(1));
					}
					count++;
				}
				query = MessageFormat.format(PluginConstants.MAXQUERY,
						PluginConstants.COMMON_PROPS.getProperty("table_jcm"),
						PluginConstants.COMMON_PROPS
								.getProperty("jcm_col_JuCICode"));
				rs = hsqlStatement.executeQuery(query);
				while (rs.next()) {
					count = rs.getInt(1);
				}
				HashMap<String, Integer> map = new HashMap<String, Integer>();
				map.put("count", count);
				jurisClassCode.put("count", map);
			} catch (SQLException e) {
				LOG.error(
						"HSQLHelper retrieveJurisdictionClassificationData() error: ",
						e);
			}
			closeConnection(hsqlConnection);
		}
		return jurisClassCode;
	}

	public void insertAuthorityDataandCCL(int juciCode, int authorityid,
			InsertQueryHelper iqh, InsertQueryHelper ccliqh,
			HashMap<String, String> record) {
		Connection hsqlConnection = openConnection();
		if (hsqlConnection != null) {
			try {
				PreparedStatement preparedStatement = hsqlConnection
						.prepareStatement(iqh.getQuery());
				preparedStatement.setInt(1, authorityid);
				preparedStatement.setString(2, PluginConstants.COMMON_PROPS
						.getProperty("ccl_authority_type"));
				preparedStatement.setInt(3, juciCode);
				preparedStatement.executeUpdate();
				preparedStatement = hsqlConnection.prepareStatement(ccliqh
						.getQuery());
				preparedStatement.setInt(1, authorityid);
				preparedStatement.setString(2,
						record.get(ccliqh.getFieldList().get(1)));
				preparedStatement.setString(3,
						record.get(ccliqh.getFieldList().get(2)));
				preparedStatement.setString(4,
						record.get(ccliqh.getFieldList().get(3)));
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				LOG.error("HSQLHelper insertAuthorityDataandCCL() error: ", e);
				LOG.info("Record causing problem:" + record);
			}
			closeConnection(hsqlConnection);
		}
	}

	public int getAuthCount() {
		Connection hsqlConnection = openConnection();
		int count = 0;
		if (hsqlConnection != null) {
			try {
				String query = MessageFormat.format("SELECT MAX({1}) FROM {0}",
						PluginConstants.COMMON_PROPS.getProperty("table_au"),
						PluginConstants.COMMON_PROPS
								.getProperty("au_col_AuthorityId"));
				// LOG.info("Auth Count Query="+query);
				Statement hsqlStatement = hsqlConnection.createStatement();
				ResultSet rs = hsqlStatement.executeQuery(query);
				while (rs.next()) {
					count = rs.getInt(1);
				}
			} catch (SQLException e) {
				LOG.error("HSQLHelper getAuthCount() error: ", e);
			}
			closeConnection(hsqlConnection);
		}
		return count;
	}

	public void insertJurisdiction(String jurisValue,
			String classificationValue, int jucicount, InsertQueryHelper jcmiqh) {
		Connection hsqlConnection = openConnection();
		if (hsqlConnection != null) {
			if (jcmiqh.getQuery() != null) {
				try {
					PreparedStatement preparedStatement = hsqlConnection
							.prepareStatement(jcmiqh.getQuery());
					preparedStatement.setInt(1, jucicount);
					preparedStatement.setString(2, jurisValue);
					preparedStatement.setString(3, classificationValue);
					preparedStatement.executeUpdate();
				} catch (SQLException e) {
					LOG.error("HSQLHelper insertJurisdiction() error: ", e);
				}
			}
			closeConnection(hsqlConnection);
		}
	}

	public void insertAuthorisedUsers(
			ArrayList<HashMap<String, String>> userData) {
		Connection hsqlConnection = openConnection();
		InsertQueryHelper uiqh = new InsertQueryHelper();
		uiqh.initalize(PluginConstants.COMMON_PROPS.getProperty("table_aaum"));
		uiqh.getFieldList().add(
				PluginConstants.COMMON_PROPS
						.getProperty("aaum_col_AuthorityId"));
		uiqh.getFieldList().add(
				PluginConstants.COMMON_PROPS.getProperty("aaum_col_UserId"));
		uiqh.getFieldList()
				.add(PluginConstants.COMMON_PROPS
						.getProperty("aaum_col_WindowsSID"));
		uiqh.prepareQuery();
		Pattern p = Pattern.compile("^[Ss]-[0-9]{1}-[0-9]{1}([0-9]|-)*$");
		if (hsqlConnection != null) {
			if (uiqh.getQuery() != null) {
				try {
					PreparedStatement preparedStatement = hsqlConnection
							.prepareStatement(uiqh.getQuery());
					int subDataSize = 0;
					for (HashMap<String, String> record : userData) {
						Integer authId = Integer.valueOf(record
								.get(PluginConstants.COMMON_PROPS
										.getProperty("aaum_col_AuthorityId")));
						String id = (record.get(PluginConstants.COMMON_PROPS
								.getProperty("aaum_col_UserId")));
						Matcher m = p.matcher(id);
						preparedStatement.setInt(1, authId);
						if (m.matches()) {
							preparedStatement.setString(2, null);
							if (id != null)
								preparedStatement
										.setString(3, id.toLowerCase());
							else
								preparedStatement.setString(3, id);
						} else {
							if (id != null)
								preparedStatement
										.setString(2, id.toLowerCase());
							else
								preparedStatement.setString(2, id);
							preparedStatement.setString(3, null);
						}
						preparedStatement.addBatch();
						subDataSize++;
						if (subDataSize % 1000 == 0) {
							preparedStatement.executeBatch();
							hsqlConnection.createStatement().executeUpdate(
									"COMMIT");
						}
					}
					if (subDataSize % 1000 == 1) {
						preparedStatement.executeUpdate();
						hsqlConnection.createStatement()
								.executeUpdate("COMMIT");
					} else if (subDataSize % 1000 > 1) {
						preparedStatement.executeBatch();
						hsqlConnection.createStatement()
								.executeUpdate("COMMIT");
					}

				} catch (SQLException e) {
					LOG.error("HSQLHelper insertAuthorisedUsers() error: ", e);
				}
			}
			closeConnection(hsqlConnection);
		}
	}

	public ArrayList<String> getCountryCodes() {
		Connection hsqlConnection = openConnection();
		ArrayList<String> result = null;
		if (hsqlConnection != null) {
			try {
				String query = MessageFormat.format("SELECT {1} FROM {0}",
						"COUNTRYMASTER", PluginConstants.COMMON_PROPS
								.getProperty("ucm_col_countrycode"));
				// LOG.info("Auth Count Query="+query);
				Statement hsqlStatement = hsqlConnection.createStatement();
				ResultSet rs = hsqlStatement.executeQuery(query);
				result = new ArrayList<String>();
				while (rs.next()) {
					result.add(rs.getString(1));
				}
			} catch (SQLException e) {
				LOG.error("HSQLHelper getAuthCount() error: ", e);
			}
			closeConnection(hsqlConnection);
		}
		return result;
	}
}
