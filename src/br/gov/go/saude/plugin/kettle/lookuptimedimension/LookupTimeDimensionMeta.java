package br.gov.go.saude.plugin.kettle.lookuptimedimension;

import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.SQLStatement;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

public class LookupTimeDimensionMeta extends BaseStepMeta implements StepMetaInterface {
	
	private static Class<?> PKG = LookupTimeDimensionMeta.class; // for i18n purposes
	
	// 
	// Classe que armazena principalmente os dados informados na tela do step 
	//
	
	/** database connection */
	private DatabaseMeta  databaseMeta;

	/** what's the lookup schema? */
    private String  schemaName;

	/** what's the lookup table? */
	private String  tableName;

	/** list of date types on table */
	private String dateTypes[];      

	/** list of stream fields on table */
	private String streamFields[];      

	/** store technical key name **/
	private String techKey;

	/** store output field name **/
	private String outputField;

	/** Locale Language: pt **/
	private String localeLanguage;

	/** Locale Country: BR **/
	private String localeCountry;
	
	/** Generate dimension entry for year null **/
    private boolean yearNullActive;
    
	/** Only lookup for dates, do not insert new ones **/
    private boolean onlyLookupActive;

    /** Commit size for insert / update */
	private Integer commitSize;

	/** Limit the cache size to this! */
	private int cacheSize = 9999;      

	
	public LookupTimeDimensionMeta() {
		super(); 
	}
	
	@Override
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info) {

		//TODO VERIFICAR NO FORUM PQ METODO check() NAO ESTA SENDO CHAMANDO. NEM O SORT ROWS COM UM DEBUG NAO PASSA.
		
		CheckResult cr;

		// See if we have input streams leading to this step!
		if (input.length>0)
		{
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "LookupTimeDimension.Meta.CheckResult.ReceivingInfoFromOtherSteps"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
		else
		{
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "LookupTimeDimension.Meta.CheckResult.NoInputReceived"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
	
		// also check that each expected stream fields are acually coming
		if (prev!=null && prev.size()>0)
		{
			boolean first=true;
			String error_message = ""; 
			boolean error_found = false;
			
			for (int i=0;i<dateTypes.length;i++)
			{
				ValueMetaInterface v = prev.searchValueMeta(dateTypes[i]);
				if (v==null)
				{
					if (first)
					{
						first=false;
						error_message+=BaseMessages.getString(PKG, "VoldemortStep.Check.MissingFieldsNotFoundInInput")+Const.CR;
					}
					error_found=true;
					error_message+="\t\t"+dateTypes[i]+Const.CR;
				}
			}
			if (error_found)
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			}
			else
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "VoldemortStep.Check.AllFieldsFoundInInput"), stepMeta);
			}
			remarks.add(cr);
		}
		else
		{
			String error_message=BaseMessages.getString(PKG, "VoldemortStep.Check.CouldNotReadFromPreviousSteps")+Const.CR;
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			remarks.add(cr);
		}	
		
	}

	@Override
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans) {
		return new LookupTimeDimensionStep(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}

	@Override
	public StepDataInterface getStepData() {
		return new LookupTimeDimensionData();
	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new LookupTimeDimensionDialog(shell, meta, transMeta, name);
	}
	
	// Esse metodo é responsavel por salvar no .ktr o xml que salva as configuracoes do step
	@Override
	public String getXML() throws KettleValueException {
		
		StringBuffer retval = new StringBuffer(150);
		
		retval.append("      ").append(XMLHandler.addTagValue("connection", databaseMeta==null?"":databaseMeta.getName()));
        retval.append("      ").append(XMLHandler.addTagValue("schema", schemaName));
		retval.append("      ").append(XMLHandler.addTagValue("table", tableName));
		retval.append("      ").append(XMLHandler.addTagValue("key", techKey));
		retval.append("      ").append(XMLHandler.addTagValue("commitSize", commitSize));
		retval.append("      ").append(XMLHandler.addTagValue("outputField", outputField));
		retval.append("      ").append(XMLHandler.addTagValue("language", localeLanguage));
		retval.append("      ").append(XMLHandler.addTagValue("country", localeCountry));
		retval.append("      ").append(XMLHandler.addTagValue("year_null_active", yearNullActive));
		retval.append("      ").append(XMLHandler.addTagValue("only_lookup_active", onlyLookupActive));

		for (int i=0;i<dateTypes.length;i++) {
			
			retval.append("      <fields>").append(Const.CR);

			retval.append("        ").append(XMLHandler.addTagValue("dateType", dateTypes[i]));
			retval.append("        ").append(XMLHandler.addTagValue("streamField", streamFields[i]));
		
			retval.append("      </fields>").append(Const.CR);
			
		}

		return retval.toString();
	}

	// Metodo responsavel por ler o xml do .ktr e popular o step com os dados salvos
	@Override
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException {

		try {
			
			String con = XMLHandler.getTagValue(stepnode, "connection"); //$NON-NLS-1$
			databaseMeta = DatabaseMeta.findDatabase(databases, con);
			
			schemaName = XMLHandler.getTagValue(stepnode, "schema");
			tableName = XMLHandler.getTagValue(stepnode, "table");
			techKey = XMLHandler.getTagValue(stepnode, "key");
			
			String commit = XMLHandler.getTagValue(stepnode, "commitSize"); //$NON-NLS-1$
			commitSize = Const.toInt(commit, 0);
			
			outputField = XMLHandler.getTagValue(stepnode, "outputField");
			localeLanguage = XMLHandler.getTagValue(stepnode, "language");
			localeCountry = XMLHandler.getTagValue(stepnode, "country");
			yearNullActive  = "Y".equals(XMLHandler.getTagValue(stepnode, "year_null_active"));
			onlyLookupActive  = "Y".equals(XMLHandler.getTagValue(stepnode, "only_lookup_active"));
			
			int nrKeys = XMLHandler.countNodes(stepnode, "fields"); 
			allocate(nrKeys);
			
			for (int i=0;i<nrKeys;i++)
			{
				Node knode = XMLHandler.getSubNodeByNr(stepnode, "fields", i);
				
				dateTypes[i] 	= XMLHandler.getTagValue(knode, "dateType"); 
				streamFields[i] = XMLHandler.getTagValue(knode, "streamField");
				
			}
			
		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.Exception.UnexpectedErrorWhileReadingXML"), e);
		}
		
	}

	@Override
	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException {
		
		try
		{
			databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "connection", databases);
			
			schemaName = rep.getStepAttributeString (id_step, "schema");
			tableName = rep.getStepAttributeString (id_step, "table");
			techKey = rep.getStepAttributeString (id_step, "key");
			
			commitSize = (int)rep.getStepAttributeInteger(id_step, "commitSize");
			
			outputField = rep.getStepAttributeString (id_step, "outputField");
			localeLanguage = rep.getStepAttributeString (id_step, "language");
			localeCountry = rep.getStepAttributeString (id_step, "country");
			yearNullActive  = "Y".equals(rep.getStepAttributeString (id_step, "year_null_active"));
			onlyLookupActive  = "Y".equals(rep.getStepAttributeString (id_step, "only_lookup_active"));
			
			int nrKeys = rep.countNrStepAttributes(id_step, "dateType"); 
			allocate(nrKeys);
			
			for (int i=0;i<nrKeys;i++)
			{
				dateTypes[i] 	= rep.getStepAttributeString(id_step, i, "dateType"); 
				streamFields[i] = rep.getStepAttributeString(id_step, i, "streamField");
			}
			
		}
		
		catch(Exception e)
		{
			throw new KettleException(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.Exception.UnexpectedErrorWhileReadingStepInfo"), e); //$NON-NLS-1$
		}

	}

	@Override
	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		
		rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "connection", databaseMeta);
		
        rep.saveStepAttribute(id_transformation, id_step, "schema",           schemaName);
		rep.saveStepAttribute(id_transformation, id_step, "table",            tableName);
		rep.saveStepAttribute(id_transformation, id_step, "key",              techKey);
		rep.saveStepAttribute(id_transformation, id_step, "commitSize",       commitSize);
		rep.saveStepAttribute(id_transformation, id_step, "outputField",      outputField);
		rep.saveStepAttribute(id_transformation, id_step, "language",         localeLanguage);
		rep.saveStepAttribute(id_transformation, id_step, "country",          localeCountry);
		rep.saveStepAttribute(id_transformation, id_step, "year_null_active", yearNullActive);
		rep.saveStepAttribute(id_transformation, id_step, "only_lookup_active", onlyLookupActive);

		for (int i=0;i<dateTypes.length;i++) {
			rep.saveStepAttribute(id_transformation, id_step, i, "dateType",    dateTypes[i]); //$NON-NLS-1$
			rep.saveStepAttribute(id_transformation, id_step, i, "streamField", streamFields[i]); //$NON-NLS-1$
		}

		// Also, save the step-database relationship!
		if (databaseMeta!=null) rep.insertStepDatabase(id_transformation, id_step, databaseMeta.getObjectId());
		
	}
	
	@Override
    public Object clone()
    {
        LookupTimeDimensionMeta retval = (LookupTimeDimensionMeta) super.clone();

        int nrfields = dateTypes.length;

        retval.allocate(nrfields);

        for (int i = 0; i < nrfields; i++)
        {
            retval.dateTypes[i] = dateTypes[i];
            retval.streamFields[i] = streamFields[i];
        }

        return retval;
    }

	// Quando o step eh selecionado para utilizacao, esse metodo é chamado para definir valores default para os campos do step.
	@Override
	public void setDefault() {
		techKey        = "id";
		outputField    = "output_id";
		databaseMeta   = null;
		schemaName     = "";
		tableName      = BaseMessages.getString(PKG, "LookupTimeDimension.Meta.TimeDimensionTableName.Label");
		localeLanguage = "%%user.language%%";
		localeCountry  = "%%user.country%%";
		yearNullActive = false;
		onlyLookupActive = false;
		commitSize     = 100;
		
        int nrfields = 0;

        allocate(nrfields);

        for (int i = 0; i < nrfields; i++)
        {
            dateTypes[i] = "date" + i;
        	streamFields[1] = "field" + 1;
        }

	}
	
    public void allocate(int nrfields)
    {
    	dateTypes = new String[nrfields]; // order by
    	streamFields = new String[nrfields];
    }

    // Quando por exemplo faz um preview ou 'campos de saida', o kettle precisa saber quais campos serao os outputs. Para isso esse metodo eh chamado
    // onde pelo r os campos que chegam do step anterior e adiciona os novos campos que serao incluidos.
	@Override
	public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {

		// append the outputField to the output
		ValueMetaInterface v = new ValueMeta();
		v.setName(outputField);
		v.setType(ValueMeta.TYPE_INTEGER);
		v.setTrimType(ValueMeta.TRIM_TYPE_BOTH);
		v.setOrigin(origin);

		r.addValueMeta(v);
		
	}
	
	// Metodo utilizado apenas para o botao SQL
	public SQLStatement getSQLStatements(TransMeta transMeta, StepMeta stepMeta, RowMetaInterface fields)
	{
		SQLStatement retval = new SQLStatement(stepMeta.getName(), databaseMeta, null); // default: nothing to do!

		if (databaseMeta!=null)
		{
			if (fields!=null && fields.size()>0)
			{
				if (!Const.isEmpty(tableName))
				{
                    String schemaTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, tableName);
					Database db = new Database(loggingObject, databaseMeta);
					try
					{
						String cr_table = null;

						db.connect();

						// Get technical key name ...
						ValueMetaInterface vkeyfield = new ValueMeta(techKey, ValueMetaInterface.TYPE_INTEGER);
						vkeyfield.setLength(8);
                        vkeyfield.setPrecision(0);

						// Add technical key field.
						fields.addValueMeta(0, vkeyfield);				
							
						cr_table = db.getDDL(schemaTable, fields);
						cr_table+=Const.CR;

						//
						// OK, now let's build the index
						//

						// Create unique index ...
						String cr_uniq_index = "";
						if ( !Const.isEmpty(techKey))
						{
							String techKeyArr[] = new String [] { techKey };
							if (!db.checkIndexExists(schemaName, tableName, techKeyArr))
							{
								String indexname = "idx_"+tableName+"_pk"; //$NON-NLS-1$ //$NON-NLS-2$
								cr_uniq_index = db.getCreateIndexStatement(schemaName, tableName, indexname, techKeyArr, true, true, false, true);
								cr_uniq_index+=Const.CR;
							}
						}

						// Create Lookup Index
						String cr_index = "";
						String idx_fields[] = null;
						
						if ( fields != null && fields.size() > 0 )
						{
							int nrfields = fields.size();
							int maxFields = databaseMeta.getMaxColumnsInIndex();
							
							if (maxFields > 0 && nrfields > maxFields) {
								nrfields=maxFields;  // For example, oracle indexes are limited to 32 fields...
							}
							
							idx_fields = new String[nrfields-1];
							int j = 0;
							for (int i = 0 ; i < nrfields ; i++) {
								
								String fieldName = fields.getValueMeta(i).getName();
								
								if (!Const.isEmpty(fieldName))
									if (!fieldName.equalsIgnoreCase(techKey))
										idx_fields[j++] = fieldName;
								
							}
						}
						else
						{
							retval.setError(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.ReturnValue.NotFieldsSpecified")); //$NON-NLS-1$
						}

						
						// OK, now get the create lookup index statement...
						if (!Const.isEmpty(idx_fields) && !db.checkIndexExists(schemaName, tableName, idx_fields)
						)
						{
							String indexname = "idx_"+tableName+"_lookup";
							cr_index = db.getCreateIndexStatement(schemaName, tableName, indexname, idx_fields, false, false, false, true);
							cr_index+=Const.CR;
						}
						
						retval.setSQL(transMeta.environmentSubstitute(cr_table+cr_uniq_index+cr_index));
						
					}
					catch(KettleException e)
					{
						retval.setError(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.Error.FieldNotFound")+Const.CR+e.getMessage()); //$NON-NLS-1$
					}
				}
				else
				{
					retval.setError(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.ReturnValue.NotTableDefined")); //$NON-NLS-1$
				}
			}
			else
			{
				retval.setError(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.ReturnValue.NotReceivingField")); //$NON-NLS-1$
			}
		}
		else
		{
			retval.setError(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.ReturnValue.NotConnectionDefined")); //$NON-NLS-1$
		}

		return retval;
	}
	
	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getLocaleLanguage() {
		return localeLanguage;
	}

	public void setLocaleLanguage(String localeLanguage) {
		this.localeLanguage = localeLanguage;
	}

	public String getLocaleCountry() {
		return localeCountry;
	}

	public void setLocaleCountry(String localCountry) {
		this.localeCountry = localCountry;
	}

	/**
	 * @return Returns the database.
	 */
	public DatabaseMeta getDatabaseMeta()
	{
		return databaseMeta;
	}

	/**
	 * @param database The database to set.
	 */
	public void setDatabaseMeta(DatabaseMeta database)
	{
		this.databaseMeta = database;
	}

	/**
	 * @return Returns the keyField (names in the stream).
	 */
	public String[] getDateTypes()
	{
		return dateTypes;
	}

	/**
	 * @param keyField The keyField to set.
	 */
	public void setDateTypes(String[] dateTypes)
	{
		this.dateTypes = dateTypes;
	}

	public String getOutputField() {
		return outputField;
	}

	public void setOutputField(String outputField) {
		this.outputField = outputField;
	}

	public String[] getStreamFields() {
		return streamFields;
	}

	public void setStreamFields(String streamFields[]) {
		this.streamFields = streamFields;
	}

	public boolean isYearNullActive() {
		return yearNullActive;
	}

	public void setYearNullActive(boolean yearNullActive) {
		this.yearNullActive = yearNullActive;
	}

	public boolean isOnlyLookupActive() {
		return onlyLookupActive;
	}

	public void setOnlyLookupActive(boolean onlyLookupActive) {
		this.onlyLookupActive = onlyLookupActive;
	}

	public Integer getCommitSize() {
		return commitSize;
	}

	public void setCommitSize(int commitSize) {
		this.commitSize = commitSize;
	}

	public String getTechKey() {
		return techKey;
	}

	public void setTechKey(String techKey) {
		this.techKey = techKey;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

}
