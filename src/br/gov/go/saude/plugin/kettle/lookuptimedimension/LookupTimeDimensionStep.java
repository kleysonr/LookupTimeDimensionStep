package br.gov.go.saude.plugin.kettle.lookuptimedimension;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import br.gov.go.saude.plugin.kettle.lookuptimedimension.LookupTimeDimensionData;
import br.gov.go.saude.plugin.kettle.lookuptimedimension.LookupTimeDimensionMeta;

public class LookupTimeDimensionStep extends BaseStep implements StepInterface {
	
	private static Class<?> PKG = LookupTimeDimensionStep.class; // for i18n purposes
	
	private LookupTimeDimensionData data;
	private LookupTimeDimensionMeta meta;

	public LookupTimeDimensionStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
		
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
		
		meta = (LookupTimeDimensionMeta)getStepMeta().getStepMetaInterface();
		data = (LookupTimeDimensionData)stepDataInterface;
		
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		
		Date initial_date;

		Object[] r = getRow(); // get row, blocks when needed!
		if (r == null) // no more input to be expected...
		{
			setOutputDone();
			return false;
		}

		if (first) 
		{
			first = false;

			data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
			
			data.schemaTable = meta.getDatabaseMeta().getQuotedSchemaTableCombination(data.realSchemaName, data.realTableName);

            // Check if the fields are coming
            data.fieldsTableNrs = new int[meta.getStreamFields().length];

            for (int i=0;i<meta.getStreamFields().length;i++)
            {
                data.fieldsTableNrs[i]=getInputRowMeta().indexOfValue(meta.getStreamFields()[i]);
                if (data.fieldsTableNrs[i]<0) // couldn't find field!
                {
                    throw new KettleStepException(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Error.FieldNotFound", meta.getStreamFields()[i]));
                }
            }
            
            // Determine the metadata row to calculate hashcodes.
            //
            data.hashRowMeta = new RowMeta();
            data.hashRowMeta = data.metaAttrs;
            
            setCombiLookup(getInputRowMeta());
            
			logBasic("Lookup Time Dimension step initialized successfully");

		}
		
		// Se Ano é null criar entrada na dimensao com ID = 0 e restante dos campos null
		
		// Calcula a data baseado nos Inputs
		initial_date = calcInitialDate(r);
		
		if (initial_date == null && meta.isYearNullActive()) {

			lookupInsertValues(data.metaAttrs, r);
			return true;
			
		}		
		else if (initial_date != null) {
			
			lookupInsertValues(data.metaAttrs, calcDimAtt(initial_date), r);

			if (checkFeedback(getLinesRead())) {
				logBasic("Linenr " + getLinesRead()); // Some basic logging
			}

			return true;
			
		} else {
			
			return false;
			
		}
		
	}

	private boolean lookupInsertValues(RowMetaInterface dataRowMeta, Map<String, Object> dataRowValues, Object[] r) throws KettleStepException {
		
		try {
			
			Object rowValues[] = new Object[dataRowMeta.size()];
			
			for (int i = 0; i < dataRowMeta.size(); i++) {
				
				rowValues[i] = dataRowValues.get(dataRowMeta.getValueMeta(i).getName());
				
			}
			
			// Long val_key = Long.parseLong(dataRowValues.get("year4") + ((Integer) dataRowValues.get("month_number")<10?"0":"") + dataRowValues.get("month_number") + ((Integer) dataRowValues.get("day_in_month")<10?"0":"") + dataRowValues.get("day_in_month"));
			Long val_key = Long.parseLong(dataRowValues.get("year4") + "" + String.format("%02d", dataRowValues.get("month_number")) + "" + String.format("%02d", dataRowValues.get("day_in_month")));
			
			
			// Verifica se chave ja esta no cache
			Long val_key_cache = lookupInCache(data.hashRowMeta, rowValues);
			
			Object[] outputRow;
			if (val_key_cache == null) { // Se nao esta faz LookupInsert no Banco
				outputRow = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, lookupValues(dataRowMeta, rowValues, val_key));
			} else {
				outputRow = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, val_key_cache);
			}
			putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
			
			/*
			Object[] outputRow = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, lookupValues(dataRowMeta, rowValues, val_key));
			putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
			*/
			
            if (checkFeedback(getLinesRead())) {
            	
            	if(log.isBasic()) logBasic(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.LineNumber")+getLinesRead());
            	
            }
		}
		
		catch(KettleException e) {
			
			  logError(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.ErrorInStepRunning")+e.getMessage()); //$NON-NLS-1$
			  setErrors(1);
			  stopAll();
			  setOutputDone();  // signal end to receiver(s)
			  return false;
		  
		}
		
		return true;

	}
	
	private boolean lookupInsertValues(RowMetaInterface dataRowMeta, Object[] r) throws KettleStepException {
		
		try {
			
			Object rowValues[] = new Object[dataRowMeta.size()];
			
			for (int i = 0; i < dataRowMeta.size(); i++) {
				
				rowValues[i] = null;
				
			}
			
			Long val_key = 0L;
			
			Object[] outputRow = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, lookupValues(dataRowMeta, rowValues, val_key));
			putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)

            if (checkFeedback(getLinesRead())) {
            	
            	if(log.isBasic()) logBasic(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.LineNumber")+getLinesRead());
            	
            }
		}
		
		catch(KettleException e) {
			
			  logError(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.ErrorInStepRunning")+e.getMessage()); //$NON-NLS-1$
			  setErrors(1);
			  stopAll();
			  setOutputDone();  // signal end to receiver(s)
			  return false;
		  
		}
		
		return true;

	}

    private Long lookupValues(RowMetaInterface rowMeta, Object[] row, Long val_key) throws KettleException
	{
        
        Object[] lookupRow = new Object[data.lookupRowMeta.size()];
        int lookupIndex = 0;
        
        int nrfields = data.metaAttrs.size();
		for (int i = 0 ; i < nrfields ; i++) {
			
			// Determine the index of this Key Field in the row meta/data
			lookupRow[lookupIndex] = row[i]; // KEYi = ?
			lookupIndex++;

			if( meta.getDatabaseMeta().requiresCastToVariousForIsNull() && rowMeta.getValueMeta(i).getType() == ValueMeta.TYPE_STRING ) {
				
				lookupRow[lookupIndex] = rowMeta.getValueMeta(i).isNull(row[i]) ? null : "NotNull"; // KEYi IS NULL or ? IS NULL
				
			} else {
				
				lookupRow[lookupIndex] = row[i]; // KEYi IS NULL or ? IS NULL
				
			}
			lookupIndex++;
		}

		data.db.setValues(data.lookupRowMeta, lookupRow, data.prepStatementLookup);
		Object[] add = data.db.getLookup(data.prepStatementLookup);
        incrementLinesInput();

		if (add==null) // The dimension entry was not found, we need to add it!
		{
			
			if (meta.isOnlyLookupActive()) {
				throw new KettleStepException("Error: Key " + val_key + " not found on dimension table.");
			}

			combiInsert( rowMeta, row, val_key );
			incrementLinesOutput();

            if (log.isRowLevel()) logRowlevel(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.AddedDimensionEntry")+val_key); //$NON-NLS-1$

		}
		else
		{
            // Entry already exists...
			val_key = data.db.getReturnRowMeta().getInteger(add, 0); // Sometimes it's not an integer, believe it or not.
		}
		
		// Also store it in our Hashtable...
		addToCache(rowMeta, row, val_key);

		return val_key;
		
	}

	// Calcula data inicial baseado nos parametros informados no step
	private Date calcInitialDate(Object[] row) throws KettleStepException {

        // Check for the type of date defined
		Integer _anoValor = null;
		int _ano = Arrays.asList(meta.getDateTypes()).indexOf(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year"));
		if (_ano >= 0) {
			_ano = getInputRowMeta().indexOfValue(meta.getStreamFields()[_ano]);
			if (row[_ano] == null && !meta.isYearNullActive()) {
				throw new KettleStepException("Field " + BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year") + " defined. Can not be null.");
			} else if (row[_ano] == null) {
				return null;
			}
			_anoValor = ((Long) row[_ano]).intValue(); 
		}
        
		Integer _mesValor = null;
		int _mes = Arrays.asList(meta.getDateTypes()).indexOf(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Month"));
		if (_mes >= 0) {
			_mes = getInputRowMeta().indexOfValue(meta.getStreamFields()[_mes]);			
			if (row[_mes] == null) {
				throw new KettleStepException("Field " + BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year") + " defined. Can not be null.");
			}
			_mesValor = ((Long) row[_mes]).intValue(); 
		}

		Integer _semanaValor = null;
		int _semana = Arrays.asList(meta.getDateTypes()).indexOf(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Week"));
		if (_semana >= 0) {
			_semana = getInputRowMeta().indexOfValue(meta.getStreamFields()[_semana]);			
			if (row[_semana] == null) {
				throw new KettleStepException("Field " + BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year") + " defined. Can not be null.");
			}
			_semanaValor = ((Long) (Long) row[_semana]).intValue(); 
		}

		Integer _diaValor = null;
		int _dia = Arrays.asList(meta.getDateTypes()).indexOf(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Day"));
		if (_dia >= 0) {
			_dia = getInputRowMeta().indexOfValue(meta.getStreamFields()[_dia]);			
			if (row[_dia] == null) {
				throw new KettleStepException("Field " + BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year") + " defined. Can not be null.");
			}
			_diaValor = ((Long) (Long) row[_dia]).intValue(); 
		}

		Integer _bimestreValor = null;
		int _bimestre = Arrays.asList(meta.getDateTypes()).indexOf(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Bimonthly"));
		if (_bimestre >= 0) {
			_bimestre = getInputRowMeta().indexOfValue(meta.getStreamFields()[_bimestre]);			
			if (row[_bimestre] == null) {
				throw new KettleStepException("Field " + BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year") + " defined. Can not be null.");
			}
			_bimestreValor = ((Long) (Long) row[_bimestre]).intValue(); 
		}

		Integer _trimestreValor = null;
		int _trimestre = Arrays.asList(meta.getDateTypes()).indexOf(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Quarter"));
		if (_trimestre >= 0) {
			_trimestre = getInputRowMeta().indexOfValue(meta.getStreamFields()[_trimestre]);			
			if (row[_trimestre] == null) {
				throw new KettleStepException("Field " + BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year") + " defined. Can not be null.");
			}
			_trimestreValor = ((Long) (Long) row[_trimestre]).intValue(); 
		}

		Integer _semestreValor = null;
		int _semestre = Arrays.asList(meta.getDateTypes()).indexOf(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Semester"));
		if (_semestre >= 0) {
			_semestre = getInputRowMeta().indexOfValue(meta.getStreamFields()[_semestre]);			
			if (row[_semestre] == null) {
				throw new KettleStepException("Field " + BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year") + " defined. Can not be null.");
			}
			_semestreValor = ((Long) (Long) row[_semestre]).intValue(); 
		}

    	//Create a Locale according to the specified language code
    	Locale locale = new Locale(data.realLocaleLanguage, data.realLocaleCountry);
    	
    	//Create a calendar, use the first day of the year
    	Calendar cal = new GregorianCalendar(locale);

    	// Data completa
        if ( _ano >= 0 && _mes >= 0 && _dia >= 0) { 

        	//Clean the default calendar
        	cal.clear();

        	//Set date
        	cal.set(_anoValor, (_mesValor - 1), _diaValor); //month is zero based
        	
    		return cal.getTime();

        } 
        
        // Assume o primeiro dia do mes
        else if ( _ano >= 0 && _mes >= 0 ) {

        	//Clean the default calendar
        	cal.clear();

        	//Set date
        	cal.set(_anoValor, (_mesValor - 1), 1); //month is zero based
        	
    		return cal.getTime();

        }
        
        // Calcula o mes referente a semana e assume o primeiro dia do mes
        else if ( _ano >= 0 && _semana >= 0) {
        	
        	if (_semanaValor < 1 || _semanaValor > 53) { 
        		throw new KettleStepException("Week must be between 1 and 53.");
        	}
        	
        	//Clean the default calendar
        	cal.clear();

        	//set the calendar to the year
        	cal.set(Calendar.YEAR, _anoValor);

        	//set the calendar to the week
        	cal.set(Calendar.WEEK_OF_YEAR, _semanaValor);
        	
        	return cal.getTime();
        	
        }
        
        // Assume o primeiro mes do bimestre e primeiro dia do mes
        else if ( _ano >= 0 && _bimestre >= 0) { 

        	//Clean the default calendar
        	cal.clear();

        	int mes_number = 0;
        	switch(_bimestreValor){
            	case 1: mes_number = 1; break;
        	    case 2: mes_number = 3; break;
        	    case 3: mes_number = 5; break;
        	    case 4: mes_number = 7; break;
        	    case 5: mes_number = 9; break;
        	    case 6: mes_number = 11; break;
        	}

        	if (mes_number == 0) { 
        		throw new KettleStepException("Invalid Bimonthly.");
        	}
        	
        	//Set date
        	cal.set(_anoValor, (mes_number - 1), 1); //month is zero based

        	return cal.getTime();

        }
        
        // Assume o primeiro mes do trimestre e primeiro dia do mes
        else if ( _ano >= 0 && _trimestre >= 0) { 

        	//Clean the default calendar
        	cal.clear();

        	int mes_number = 0;
        	switch(_trimestreValor){
            	case 1: mes_number = 1; break;
        	    case 2: mes_number = 4; break;
        	    case 3: mes_number = 7; break;
        	    case 4: mes_number = 10; break;
        	}

        	if (mes_number == 0) { 
        		throw new KettleStepException("Invalid Quarter.");
        	}
        	
        	//Set date
        	cal.set(_anoValor, (mes_number - 1), 1); //month is zero based

        	return cal.getTime();

        }

        // Assume o primeiro mes do semestre e primeiro dia do mes
        else if ( _ano >= 0 && _semestre >= 0) { 

        	//Clean the default calendar
        	cal.clear();

        	int mes_number = 0;
        	switch(_semestreValor){
            	case 1: mes_number = 1; break;
        	    case 2: mes_number = 7; break;
        	}

        	if (mes_number == 0) { 
        		throw new KettleStepException("Invalid Semester.");
        	}
        	
        	//Set date
        	cal.set(_anoValor, (mes_number - 1), 1); //month is zero based

        	return cal.getTime();

        }

        // Assume o primeiro mes do ano e primeiro dia do mes
        else if ( _ano >= 0 ) { 

        	//Clean the default calendar
        	cal.clear();

        	//Set date
        	cal.set(_anoValor, (1 - 1), 1); //month is zero based

        	return cal.getTime();

        }
        
        return null;

	}

	private void addValor(Map<String, Object> map, String chave, Object valor) throws KettleStepException {

		if (map.containsKey(chave)) {
			map.put(chave, valor);
		} else {
			throw new KettleStepException(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Error.AddValue"));
		}

	}
	
	// Calculate dimension attributes
	private Map<String, Object> calcDimAtt(Date date) throws KettleStepException {
		
		Map <String, Object> dateAttrs = new HashMap<String, Object>(data.mapAttrs);
		
    	//Create a Locale according to the specified language code
    	Locale locale = new Locale(data.realLocaleLanguage, data.realLocaleCountry);
    	
    	//Create a calendar, use the first day of the year
    	Calendar cal = new GregorianCalendar(locale);

    	// Set for calculated date
    	cal.setTime(date);
    	
    	// en-us example: Sep 3, 2007 
    	String date_medium = DateFormat.getDateInstance(DateFormat.MEDIUM, locale).format(cal.getTime());
    	addValor(dateAttrs, "date_medium", date_medium);
    	
    	// en-us example: September 3, 2007
    	String date_long = DateFormat.getDateInstance(DateFormat.LONG, locale).format(cal.getTime());
    	addValor(dateAttrs, "date_long", date_long);
    	
    	// en-us example: Monday, September 3, 2007
    	String date_full = DateFormat.getDateInstance(DateFormat.FULL, locale).format(cal.getTime());
    	addValor(dateAttrs, "date_full", date_full);
    	
    	// day in year: 1..366
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("D",locale);
    	Long day_in_year = Long.parseLong(simpleDateFormat.format(date));
    	addValor(dateAttrs, "day_in_year", day_in_year);
    	
    	// day in month: 1..31
    	simpleDateFormat.applyPattern("d");
    	Long day_in_month = Long.parseLong(simpleDateFormat.format(date));
    	addValor(dateAttrs, "day_in_month", day_in_month);

    	// en-us example: "Monday"
    	simpleDateFormat.applyPattern("EEEE");
    	String day_name = simpleDateFormat.format(date);
    	addValor(dateAttrs, "day_name", day_name);

    	// en-us example: "Mon"
    	simpleDateFormat.applyPattern("E");
    	String day_abbreviation = simpleDateFormat.format(date);
    	addValor(dateAttrs, "day_abbreviation", day_abbreviation);


    	// week in month, 1..5
    	simpleDateFormat.applyPattern("W");
    	Long week_in_month = Long.parseLong(simpleDateFormat.format(date));
    	addValor(dateAttrs, "week_in_month", week_in_month);
    	
    	// month number in year, 1..12
    	simpleDateFormat.applyPattern("MM");
    	Long month_number = Long.parseLong(simpleDateFormat.format(date));
    	addValor(dateAttrs, "month_number", month_number);

    	// en-us example: "September"
    	simpleDateFormat.applyPattern("MMMM");
    	String month_name = simpleDateFormat.format(date);
    	addValor(dateAttrs, "month_name", month_name);

    	// en-us example: "Sep"
    	simpleDateFormat.applyPattern("MMM");
    	String month_abbreviation = simpleDateFormat.format(date);
    	addValor(dateAttrs, "month_abbreviation", month_abbreviation);

    	// 4 digit representation of the year, example:  2007
    	simpleDateFormat.applyPattern("yyyy");
    	Long year4 = Long.parseLong(simpleDateFormat.format(date));
    	addValor(dateAttrs, "year4", year4);

    	// week in year, 1..53
    	simpleDateFormat.applyPattern("ww");
    	Long week_in_year = Long.parseLong(simpleDateFormat.format(date));
    	addValor(dateAttrs, "week_in_year", week_in_year);

    	// For some dates week 1 is in the next year
    	Long year_of_week = null;
    	if (day_in_year > 7 && week_in_year == 1) {
    		year_of_week = year4 + 1;
    	} else {
    		year_of_week = year4;
    	}
    	addValor(dateAttrs, "year_of_week", year_of_week);
    	
    	// en-us example: 9/3/2007 (but using yyyy as year display)
        DateFormat dateFormat = DateFormat.getDateInstance(DateFormat.SHORT, locale);
        SimpleDateFormat simple = (SimpleDateFormat) dateFormat;
        String pattern = simple.toPattern().replaceAll("\\byy\\b", "yyyy");
        simpleDateFormat.applyPattern(pattern);
        String date_short = simpleDateFormat.format(date);
        addValor(dateAttrs, "date_short", date_short);


    	// handling Bimestres is a DIY
    	String bimestre_name = BaseMessages.getString(PKG, "LookupTimeDimension.Step.Attributes.Bimonthly.Name");
    	Long bimestre_number = 0L;
    	switch(month_number.intValue()){
    	    case 1:  case 2:  bimestre_number  = 1L; break;
    	    case 3:  case 4:  bimestre_number  = 2L; break;
    	    case 5:  case 6:  bimestre_number  = 3L; break;
    	    case 7:  case 8:  bimestre_number  = 4L; break;
    	    case 9:  case 10: bimestre_number  = 5L; break;
    	    case 11: case 12: bimestre_number  = 6L; break;
    	}
    	bimestre_name += bimestre_number;
    	addValor(dateAttrs, "bimonthly_number", bimestre_number);
    	addValor(dateAttrs, "bimonthly_name", bimestre_name);

    	//handling Trimestres is a DIY
    	String trimestre_name = BaseMessages.getString(PKG, "LookupTimeDimension.Step.Attributes.Quarter.Name");
    	Long trimestre_number = 0L;
    	switch(month_number.intValue()){
    	    case 1:  case 2:  case 3: trimestre_number  = 1L; break;
    	    case 4:  case 5:  case 6: trimestre_number  = 2L; break;
    	    case 7:  case 8:  case 9: trimestre_number  = 3L; break;
    	    case 10: case 11: case 12: trimestre_number = 4L; break;
    	}
    	trimestre_name += trimestre_number;
    	addValor(dateAttrs, "quarter_number", trimestre_number);
    	addValor(dateAttrs, "quarter_name", trimestre_name);

    	// handling Semesters is a DIY
    	String semestre_name = BaseMessages.getString(PKG, "LookupTimeDimension.Step.Attributes.Semester.Name");
    	Long semestre_number = 0L;
    	switch(month_number.intValue()){
    	    case 1:  case 2:  case 3: case 4:   case 5:   case 6:  semestre_number  = 1L; break;
    	    case 7:  case 8:  case 9: case 10:  case 11:  case 12: semestre_number  = 2L; break;
    	}
    	semestre_name += semestre_number;
    	addValor(dateAttrs, "semester_number", semestre_number);
    	addValor(dateAttrs, "semester_name", semestre_name);

    	// get the local yes/no values
    	String yes = BaseMessages.getString(PKG, "LookupTimeDimension.Step.Attributes.Local.Yes");
    	String no = BaseMessages.getString(PKG, "LookupTimeDimension.Step.Attributes.Local.No");

    	// initialize for week calculations
    	int first_day_of_week = cal.getFirstDayOfWeek();
    	int day_of_week = Calendar.DAY_OF_WEEK;

    	// find out if this is the first day of the week
    	String is_first_day_in_week;
    	if(first_day_of_week == cal.get(day_of_week)){
    	    is_first_day_in_week = yes;
    	} else {
    	    is_first_day_in_week = no;
    	}
    	addValor(dateAttrs, "is_first_day_in_week", is_first_day_in_week);
    	
    	// calculate the next day
    	cal.add(Calendar.DAY_OF_MONTH, 1);
    	
    	// get the next calendar date
    	Date next_day = cal.getTime();

    	// find out if this is the first day of the week
    	String is_last_day_in_week;
    	if (first_day_of_week == cal.get(day_of_week)) {
    	    is_last_day_in_week = yes;
    	} else {
    	    is_last_day_in_week = no;
    	}
    	addValor(dateAttrs, "is_last_day_in_week", is_last_day_in_week);

    	//find out if this is the first day of the month
    	String is_first_day_of_month;
    	if (day_in_month == 1) {
    		is_first_day_of_month = yes;
    	} else {
    		is_first_day_of_month = no;
    	}
    	addValor(dateAttrs, "is_first_day_of_month", is_first_day_of_month);

    	// find out if this is the last day in the month
    	String is_last_day_of_month;
    	simpleDateFormat.applyPattern("d");
    	if(Integer.parseInt(simpleDateFormat.format(next_day)) == 1) {
    	    is_last_day_of_month = yes;
    	} else {
    	    is_last_day_of_month = no;
    	}
    	addValor(dateAttrs, "is_last_day_of_month", is_last_day_of_month);

    	String year_bimonthly          = year4 + "-" + bimestre_name;
    	String year_quarter            = year4 + "-" + trimestre_name;
    	String year_semester           = year4 + "-" + semestre_name;
    	//String year_month_number       = year4 + "-" + month_number;
    	String year_month_number       = year4 + "" + String.format("%02d", month_number);
    	String year_month_abbreviation = year4 + "-" + month_abbreviation;

    	addValor(dateAttrs, "year_bimonthly", year_bimonthly);
    	addValor(dateAttrs, "year_quarter", year_quarter);
    	addValor(dateAttrs, "year_semester", year_semester);
    	addValor(dateAttrs, "year_month_number", year_month_number);
    	addValor(dateAttrs, "year_month_abbreviation", year_month_abbreviation);

    	return dateAttrs;

	}
	
    /**
     * Usado para LookupUpdate
     * 
     * table: dimension table
     * keys[]: which dim-fields do we use to look up key?
     * retval: name of the key to return
     */
    public void setCombiLookup(RowMetaInterface inputRowMeta) throws KettleDatabaseException
    {
        DatabaseMeta databaseMeta = meta.getDatabaseMeta();
        
        String sql = "";
        boolean comma;
        data.lookupRowMeta = new RowMeta();
        
        /* 
         * SELECT <retval> 
         * FROM   <table> 
         * WHERE  ( ( <key1> = ? ) OR ( <key1> IS NULL AND ? IS NULL ) )  
         * AND    ( ( <key2> = ? ) OR ( <key1> IS NULL AND ? IS NULL ) )  
         * ...
         * ;
         * 
         */
        
        sql += "SELECT "+databaseMeta.quoteField(meta.getTechKey())+Const.CR;
        sql += "FROM "+data.schemaTable+Const.CR;
        sql += "WHERE ";
        comma=false;
        
        sql += "( ( ";
        
        int nrfields = data.metaAttrs.size();
		for (int i = 0 ; i < nrfields ; i++) {
			
			String fieldName = data.metaAttrs.getValueMeta(i).getName();
			
            if (comma)
            {
                sql += " AND ( ( ";
            }
            else
            { 
                comma=true; 
            }
            sql += databaseMeta.quoteField(fieldName) + " = ? ) OR ( " + databaseMeta.quoteField(fieldName);
            data.lookupRowMeta.addValueMeta(data.metaAttrs.getValueMeta(i));
            
            sql += " IS NULL AND ";
            if (databaseMeta.requiresCastToVariousForIsNull()) {
                sql += "CAST(? AS VARCHAR(256)) IS NULL";
            }
            else
            {
                sql += "? IS NULL";
            }
            // Add the ValueMeta for the null check, BUT cloning needed.
            // Otherwise the field gets renamed and gives problems when referenced by previous steps.
            data.lookupRowMeta.addValueMeta(data.metaAttrs.getValueMeta(i).clone());
            
            sql += " ) )";
            sql += Const.CR;
        }
        
        try
        {
            if (log.isDebug()) logDebug("preparing combi-lookup statement:"+Const.CR+sql);
            data.prepStatementLookup=data.db.getConnection().prepareStatement(databaseMeta.stripCR(sql));
            if (databaseMeta.supportsSetMaxRows())
            {
                data.prepStatementLookup.setMaxRows(1); // alywas get only 1 line back!
            }
        }
        catch(SQLException ex) 
        {
            throw new KettleDatabaseException("Unable to prepare combi-lookup statement", ex);
        }
    }
    
    /**
     * This inserts new record into a time dimension
     */
    public void combiInsert( RowMetaInterface rowMeta, Object[] row, Long val_key ) throws KettleDatabaseException
    {
        String debug="Combination insert";
        DatabaseMeta databaseMeta = meta.getDatabaseMeta();
        try
        {
            if (data.prepStatementInsert==null) // first time: construct prepared statement
            {
                debug="First: construct prepared statement";
                
                data.insertRowMeta = new RowMeta();
                
                /* Construct the SQL statement...
                 *
                 * INSERT INTO 
                 * d_test(keyfield, keylookup[])
                 * VALUES(val_key, row values with keynrs[])
                 * ;
                 */
                 
                String sql = "";
                sql += "INSERT INTO " + data.schemaTable + ("( ");
                boolean comma=false;
    
                sql += databaseMeta.quoteField(meta.getTechKey());
                data.insertRowMeta.addValueMeta( new ValueMeta(meta.getTechKey(), ValueMetaInterface.TYPE_INTEGER));
                comma=true;
				
		        int nrfields = data.metaAttrs.size();
				for (int i = 0 ; i < nrfields ; i++) {
					if (comma) sql += ", "; 
					String fieldName = data.metaAttrs.getValueMeta(i).getName();
					sql += databaseMeta.quoteField(fieldName);
					data.insertRowMeta.addValueMeta(data.metaAttrs.getValueMeta(i));
					comma=true;
				}

                sql += ") VALUES (";
                
                comma=false;
                
                sql += '?';
                comma=true;

				for (int i = 0 ; i < nrfields ; i++) {
                    if (comma) sql += ','; else comma=true;
                    sql += '?';
                }
                
                sql += " )";
    
                String sqlStatement = sql;
                try
                {
                    logDetailed("SQL insert: "+sqlStatement);
                    data.prepStatementInsert=data.db.getConnection().prepareStatement(databaseMeta.stripCR(sqlStatement));
                }
                catch(SQLException ex) 
                {
                    throw new KettleDatabaseException("Unable to prepare combi insert statement : "+Const.CR+sqlStatement, ex);
                }
                catch(Exception ex)
                {
                    throw new KettleDatabaseException("Unable to prepare combi insert statement : "+Const.CR+sqlStatement, ex);
                }
			}
            
            debug="Create new insert row rins";
            Object[] insertRow=new Object[data.insertRowMeta.size()];
            int insertIndex = 0;
            
            insertRow[insertIndex] = val_key;
            insertIndex++;

            int nrfields = data.metaAttrs.size();
            for (int i=0;i<nrfields;i++)
            {
                insertRow[insertIndex] = row[i];
                insertIndex++;
            }
            
            if (log.isRowLevel()) logRowlevel("rins="+data.insertRowMeta.getString(insertRow));
            
            debug="Set values on insert";
            // INSERT NEW VALUE!
            data.db.setValues(data.insertRowMeta, insertRow, data.prepStatementInsert);
            
            debug="Insert row";
            data.db.insertRow(data.prepStatementInsert);
            
        }
        catch(Exception e)
        {
            logError(Const.getStackTracker(e));
            throw new KettleDatabaseException("Unexpected error in combination insert in part ["+debug+"] : "+e.toString(), e);
        }
        
    }

	
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		
		if (super.init(smi, sdi)) {
			
			// Converte se for uma variavel, se nao for retorna a string digitada
			data.realSchemaName     = environmentSubstitute(meta.getSchemaName());
			data.realTableName      = environmentSubstitute(meta.getTableName());
			data.realLocaleLanguage = environmentSubstitute(meta.getLocaleLanguage());
			data.realLocaleCountry  = environmentSubstitute(meta.getLocaleCountry());
			
			if (meta.getCacheSize()>0) {
				data.cache=new HashMap<RowMetaAndData, Long>((int)(meta.getCacheSize()*1.5));
			}
			else {
				data.cache=new HashMap<RowMetaAndData, Long>();
			}
			
			if(meta.getDatabaseMeta()==null) {
        		logError(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Init.ConnectionMissing", getStepname()));
        		return false;
        	}
			data.db=new Database(this, meta.getDatabaseMeta());
			data.db.shareVariablesWith(this);
			try
			{
				if (getTransMeta().isUsingUniqueConnections()) 
				{
					synchronized (getTrans()) { data.db.connect(getTrans().getThreadName(), getPartitionID()); }
				} 
				else 
				{
					data.db.connect(getPartitionID()); 
				}

				if (log.isDetailed()) logDetailed(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.ConnectedToDB")); //$NON-NLS-1$
				data.db.setCommit(meta.getCommitSize());

				return true;
			}
			catch(KettleDatabaseException dbe)
			{
				logError(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.UnableToConnectDB")+dbe.getMessage()); //$NON-NLS-1$
			}
			
		}
		
		return false;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (LookupTimeDimensionMeta) smi;
		data = (LookupTimeDimensionData) sdi;
		
	    if(data.db!=null) {
	        try
	        {
	            if (!data.db.isAutoCommit())
	            {
	                if (getErrors()==0)
	                {
	                    data.db.commit();
	                }
	                else
	                {
	                    data.db.rollback();
	                }
	            }
	        }
	        catch(KettleDatabaseException e)
	        {
	            logError(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Log.UnexpectedError")+" : "+e.toString()); //$NON-NLS-1$ //$NON-NLS-2$
	        }
	        finally 
	        {
	            data.db.disconnect();
	        }
	    }

		super.dispose(smi, sdi);
		
	}

	// Run is were the action happens!
	public void run() {
		logBasic("Starting to run...");
		try {
			while (processRow(meta, data) && !isStopped())
				;
		} catch (Exception e) {
			logError("Unexpected error : " + e.toString());
			logError(Const.getStackTracker(e));
			setErrors(1);
			stopAll();
		} finally {
			dispose(meta, data);
			logBasic("Finished, processing " + getLinesRead() + " rows");
			markStop();
		}
	}
	
	
	private Long lookupInCache(RowMetaInterface rowMeta, Object[] row)
	{
	    // Short circuit if cache is disabled.
        if (meta.getCacheSize() == -1) return null;

		// try to find the row in the cache...
		// 
		Long tk = (Long) data.cache.get(new RowMetaAndData(rowMeta, row));
		return tk;
	}
    
    
    /**
     * Adds a row to the cache
     * In case we are doing updates, we need to store the complete rows from the database.
     * These are the values we need to store
     * 
     * Key:
     *   - natural key fields
     * Value:
     *   - Technical key
     *   - lookup fields / extra fields (allows us to compare or retrieve)
     *   - Date_from
     *   - Date_to
     * 
     * @param row
     * @param returnValues
     * @throws KettleValueException 
     */
	
    private void addToCache(RowMetaInterface rowMeta, Object[] row, Long tk) throws KettleValueException
    {
        // Short circuit if cache is disabled.
        if (meta.getCacheSize() == -1) return;

        // store it in the cache if needed.
        data.cache.put(new RowMetaAndData(rowMeta, row), tk);
        
        // check if the size is not too big...
        // Allow for a buffer overrun of 20% and then remove those 20% in one go.
        // Just to keep performance in track.
        //
        int tenPercent = meta.getCacheSize()/10;
        if (meta.getCacheSize()>0 && data.cache.size()>meta.getCacheSize()+tenPercent)
        {
            // Which cache entries do we delete here?
            // We delete those with the lowest technical key...
            // Those would arguably be the "oldest" dimension entries.
            // Oh well... Nothing is going to be perfect here...
            // 
            // Getting the lowest 20% requires some kind of sorting algorithm and I'm not sure we want to do that.
            // Sorting is slow and even in the best case situation we need to do 2 passes over the cache entries...
            //
            // Perhaps we should get 20% random values and delete everything below the lowest but one TK.
            //
            List<RowMetaAndData> keys = new ArrayList<RowMetaAndData>(data.cache.keySet());
            int sizeBefore = keys.size();
            List<Long> samples = new ArrayList<Long>();
            
            // Take 10 sample technical keys....
            int stepsize=keys.size()/5;
            if (stepsize<1) stepsize=1; //make sure we have no endless loop
            for (int i=0;i<keys.size();i+=stepsize)
            {
                RowMetaAndData key = (RowMetaAndData) keys.get(i);
                Long value = (Long) data.cache.get(key);
                if (value!=null)
                {
                    samples.add(value);
                }
            }
            // Sort these 5 elements...
            Collections.sort(samples);
            
            // What is the smallest?
            // Take the second, not the fist in the list, otherwise we would be removing a single entry = not good.
            if (samples.size()>1) {
                data.smallestCacheKey = ((Long) samples.get(1)).longValue();
            } else { // except when there is only one sample
                data.smallestCacheKey = ((Long) samples.get(0)).longValue();
            }
            
            // Remove anything in the cache <= smallest.
            // This makes it almost single pass...
            // This algorithm is not 100% correct, but I guess it beats sorting the whole cache all the time.
            //
            for (int i=0;i<keys.size();i++)
            {
                RowMetaAndData key = (RowMetaAndData) keys.get(i);
                Long value = (Long) data.cache.get(key);
                if (value!=null)
                {
                    if (value.longValue()<=data.smallestCacheKey)
                    {
                        data.cache.remove(key); // this one has to go.
                    }
                }
            }
            
            int sizeAfter = data.cache.size();
            logDetailed("Reduced the lookup cache from "+sizeBefore+" to "+sizeAfter+" rows.");
        }
        
        if (log.isRowLevel()) logRowlevel("Cache store: key="+rowMeta.getString(row)+"    key="+tk);
    }
	
}
