package br.gov.go.saude.plugin.kettle.lookuptimedimension;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class LookupTimeDimensionData extends BaseStepData implements StepDataInterface {
	
	//
	// Classe que principalmente armazena dados que serao utilizados pelo step durante sua execucao.
	//

	public RowMetaInterface outputRowMeta;
	
	public Database db;
	
	public String schemaTable;
	
    public String realTableName;
    public String realSchemaName;
	public String realLocaleLanguage;
	public String realLocaleCountry;
	
    //public PreparedStatement prepStatementInsert;
    
	public int fieldsTableNrs[];
	
	public Map<String, Object> mapAttrs = new HashMap<String, Object>();
	public RowMetaInterface metaAttrs;

	// Usado para LookupUpdate
	public RowMetaInterface hashRowMeta;
	public RowMetaInterface lookupRowMeta;
	public PreparedStatement prepStatementLookup;
	public RowMetaInterface insertRowMeta;
	public PreparedStatement prepStatementInsert;

	// Usado para controle do cache
	public Map<RowMetaAndData, Long> cache; 
	public long smallestCacheKey;
	
    public LookupTimeDimensionData()
	{
		super();
		
		mapAttrs.put("date_medium", ValueMeta.TYPE_STRING);
		mapAttrs.put("date_long", ValueMeta.TYPE_STRING);
		mapAttrs.put("date_full", ValueMeta.TYPE_STRING);
		mapAttrs.put("day_in_year", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("day_in_month", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("day_name", ValueMeta.TYPE_STRING);
		mapAttrs.put("day_abbreviation", ValueMeta.TYPE_STRING);
		mapAttrs.put("week_in_year", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("week_in_month", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("month_number", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("month_name", ValueMeta.TYPE_STRING);
		mapAttrs.put("month_abbreviation", ValueMeta.TYPE_STRING);
		mapAttrs.put("year4", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("year_of_week", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("date_short", ValueMeta.TYPE_STRING);
		mapAttrs.put("bimonthly_number", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("bimonthly_name", ValueMeta.TYPE_STRING);
		mapAttrs.put("quarter_number", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("quarter_name", ValueMeta.TYPE_STRING);
		mapAttrs.put("semester_number", ValueMeta.TYPE_INTEGER);
		mapAttrs.put("semester_name", ValueMeta.TYPE_STRING);
		mapAttrs.put("is_first_day_in_week", ValueMeta.TYPE_STRING);
		mapAttrs.put("is_last_day_in_week", ValueMeta.TYPE_STRING);
		mapAttrs.put("is_first_day_of_month", ValueMeta.TYPE_STRING);
		mapAttrs.put("is_last_day_of_month", ValueMeta.TYPE_STRING);
		mapAttrs.put("year_bimonthly", ValueMeta.TYPE_STRING);
		mapAttrs.put("year_quarter", ValueMeta.TYPE_STRING);
		mapAttrs.put("year_semester", ValueMeta.TYPE_STRING);
		mapAttrs.put("year_month_number", ValueMeta.TYPE_STRING);
		mapAttrs.put("year_month_abbreviation", ValueMeta.TYPE_STRING);
		
		metaAttrs = getMetaAttrs(mapAttrs);
		
		db = null;
		realTableName = null;
		realSchemaName = null;
		realLocaleLanguage = null;
		realLocaleCountry = null;

	}
    
    // Retorna mapAttrs no formato de um RowMeta
	private RowMetaInterface getMetaAttrs(Map<String, Object> mapAttrs) {

		RowMetaInterface prevNew = new RowMeta();

		Map <String, Object> attrs = mapAttrs;
		
		for (Map.Entry<String, Object> item : attrs.entrySet()) {
			
			if (item != null && item.getValue() != null) {

				ValueMetaInterface valueMeta = new ValueMeta(item.getKey(), (Integer) item.getValue() );
				/*
				if (ValueMeta.TYPE_INTEGER == (Integer) item.getValue()) {
					//valueMeta.setLength(5);
					valueMeta.setPrecision(0);
					valueMeta.setConversionMask(null);
				}*/
				
				prevNew.addValueMeta(valueMeta);

			}
		}

		return prevNew;
	}


}
