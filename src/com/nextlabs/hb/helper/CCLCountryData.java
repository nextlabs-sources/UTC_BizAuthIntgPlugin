package com.nextlabs.hb.helper;import java.sql.Timestamp;import java.text.MessageFormat;import java.util.ArrayList;import java.util.HashMap;public class CCLCountryData extends DataRetrival {	public final String AUTHORITYID = PluginConstants.COMMON_PROPS			.getProperty("au_col_AuthorityId");	public final String CLASSIFCIATION = PluginConstants.COMMON_PROPS			.getProperty("acy_col_classification");	public final String JURISDICTION = PluginConstants.COMMON_PROPS			.getProperty("au_col_jurisdiction");	public CCLCountryData() {		filePath = PluginConstants.INSTALL_LOC + PluginConstants.CC_ROOT				+ PluginConstants.JAR_FOLDER + PluginConstants.APP_FOLDER				+ PluginConstants.DATA_FOLDER				+ PluginConstants.CCLCOUNTRIESFILENAME;	}	public ArrayList<HashMap<String, String>> getCCLCountries() {		ArrayList<HashMap<String, String>> cclCountryData = new ArrayList<HashMap<String, String>>();		QueryResultHelper qrh = new QueryResultHelper();		qrh.setQuery(MessageFormat.format(PluginConstants.BASEQUERY,				PluginConstants.COMMON_PROPS.getProperty("table_cclCountries")));		qrh.getTableFieldList().add(				PluginConstants.COMMON_PROPS						.getProperty("ccl_col_jurisdiction"));		qrh.getTableFieldList().add(				PluginConstants.COMMON_PROPS						.getProperty("ccl_col_classification"));		qrh.getTableFieldList()				.add(PluginConstants.COMMON_PROPS						.getProperty("ccl_col_countrycode"));		qrh.getTableFieldList().add(				PluginConstants.COMMON_PROPS						.getProperty("ccl_col_reasonforcontrol"));		qrh.getTableFieldList().add(				PluginConstants.COMMON_PROPS.getProperty("ccl_col_cclflag"));		cclCountryData = (ArrayList<HashMap<String, String>>) MSSQLHelper				.getInstance().retrieveData(qrh);		return cclCountryData;	}	@Override	public void prepareData() {		ArrayList<HashMap<String, String>> cclcountries = getCCLCountries();		if (cclcountries != null) {			responseData.put(PluginConstants.COMMON_PROPS					.getProperty("table_cclCountries"), cclcountries);		} else {			setErrorMessage(PluginConstants.DATARETRIVALFAILURE);			responseData = null;		}	}	@Override	public Timestamp getLastModifiedDate() {		return MSSQLHelper.getInstance().getLatestDBDate();	}}