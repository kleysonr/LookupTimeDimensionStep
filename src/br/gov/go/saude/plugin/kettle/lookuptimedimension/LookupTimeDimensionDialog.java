package br.gov.go.saude.plugin.kettle.lookuptimedimension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SQLStatement;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.database.dialog.SQLEditor;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import br.gov.go.saude.plugin.kettle.lookuptimedimension.LookupTimeDimensionMeta;

public class LookupTimeDimensionDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = LookupTimeDimensionMeta.class; // for i18n purposes
	
	private LookupTimeDimensionMeta input;
	
	private DatabaseMeta ci;
	
	/**
	 * List of ColumnInfo that should have the field names of the selected database table
	 */

	private CCombo      wConnection;

    private Label       wlSchema;
    private TextVar     wSchema;
    private Button		wbSchema;
    private FormData	fdbSchema;
    
	private Label       wlTable;
	private Button      wbTable;
	private TextVar     wTable;
  
    private Label       wlId;
    private Text        wId;
    
	private Label       wlCommit;
	private Text        wCommit;

	private Label       wlIdOutput;
    private Text        wIdOutput;

    private Label       wlLang;
    private TextVar     wLang;
    
    private Label       wlCountry;
    private TextVar     wCountry;
    
    private Label		wlFields;
    private TableView	wFields;

    private Label		wlYearCheck;
    private Button		wYearCheck;

    private Label		wlOnlyLookup;
    private Button		wOnlyLookup;

    private ColumnInfo[] colinf;
    private Map<String, Integer> inputFields;

    

	public LookupTimeDimensionDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		input = (LookupTimeDimensionMeta) in;
		inputFields =new HashMap<String, Integer>();
	}

	@Override
	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, input);

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.Title")); 

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;
		
		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				input.setChanged();
			}
		};
		ModifyListener lsTableMod = new ModifyListener() {
			public void modifyText(ModifyEvent arg0) {
				input.setChanged();
			}
		};
		SelectionListener lsSelection = new SelectionAdapter()
		{
			public void widgetSelected(SelectionEvent e) 
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();
		ci = input.getDatabaseMeta();

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName")); 
		props.setLook(wlStepname);
		
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		
	    /*************************************************
        // DB SETTINGS
		*************************************************/
		
		// Connection line
		wConnection = addConnectionLine(shell, wStepname, middle, margin);
		if (input.getDatabaseMeta()==null && transMeta.nrDatabases()==1) wConnection.select(0);
		wConnection.addModifyListener(lsMod);
		wConnection.addSelectionListener(lsSelection);
		wConnection.addModifyListener(new ModifyListener()
			{
				public void modifyText(ModifyEvent e)
				{
					// We have new content: change ci connection:
					ci = transMeta.findDatabase(wConnection.getText());
					input.setChanged();
				}
			}
		);
		
        // Schema line...
        wlSchema=new Label(shell, SWT.RIGHT);
        wlSchema.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TargetSchema.Label")); //$NON-NLS-1$
        props.setLook(wlSchema);
        FormData fdlSchema = new FormData();
        fdlSchema.left = new FormAttachment(0, 0);
        fdlSchema.right= new FormAttachment(middle, -margin);
        fdlSchema.top  = new FormAttachment(wConnection, margin);
        wlSchema.setLayoutData(fdlSchema);

		wbSchema=new Button(shell, SWT.PUSH | SWT.CENTER);
 		props.setLook(wbSchema);
 		wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
 		fdbSchema=new FormData();
 		fdbSchema.top  = new FormAttachment(wConnection, margin);
 		fdbSchema.right= new FormAttachment(100, 0);
		wbSchema.setLayoutData(fdbSchema);

        wSchema=new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wSchema);
        wSchema.addModifyListener(lsTableMod);
        FormData fdSchema = new FormData();
        fdSchema.left = new FormAttachment(middle, 0);
        fdSchema.top  = new FormAttachment(wConnection, margin);
        fdSchema.right= new FormAttachment(wbSchema, -margin);
        wSchema.setLayoutData(fdSchema);


        // Table line...
		wlTable = new Label(shell, SWT.RIGHT);
		wlTable.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.Target.Label")); //$NON-NLS-1$
 		props.setLook(wlTable);
		FormData fdlTable = new FormData();
		fdlTable.left = new FormAttachment(0, 0);
		fdlTable.right = new FormAttachment(middle, -margin);
		fdlTable.top = new FormAttachment(wbSchema, margin );
		wlTable.setLayoutData(fdlTable);

		wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
 		props.setLook(wbTable);
 		wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
		FormData fdbTable = new FormData();
		fdbTable.right = new FormAttachment(100, 0);
		fdbTable.top = new FormAttachment(wbSchema, margin);
		wbTable.setLayoutData(fdbTable);

		wTable = new TextVar(transMeta,shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wTable);
		wTable.addModifyListener(lsTableMod);
		FormData fdTable = new FormData();
		fdTable.left = new FormAttachment(middle, 0);
		fdTable.top = new FormAttachment(wbSchema, margin );
		fdTable.right = new FormAttachment(wbTable, -margin);
		wTable.setLayoutData(fdTable);		

        // Technical key line...
		wlId = new Label(shell, SWT.RIGHT);
		wlId.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.Table.Id.Label")); //$NON-NLS-1$
 		props.setLook(wlId);
		FormData fdlId = new FormData();
		fdlId.left = new FormAttachment(0, 0);
		fdlId.right = new FormAttachment(middle, -margin);
		fdlId.top = new FormAttachment(wbTable, margin );
		wlId.setLayoutData(fdlId);

		wId = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wId);
		wId.addModifyListener(lsMod);
		FormData fdId = new FormData();
		fdId.left = new FormAttachment(middle, 0);
		fdId.top = new FormAttachment(wTable, margin );
		fdId.right = new FormAttachment(100, 0);
		wId.setLayoutData(fdId);

		// Commit size line...
		wlCommit = new Label(shell, SWT.RIGHT);
		wlCommit.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Step.Commitsize.Label")); //$NON-NLS-1$
 		props.setLook(wlCommit);
		FormData fdlCommit = new FormData();
		fdlCommit.left = new FormAttachment(0, 0);
		fdlCommit.right = new FormAttachment(middle, -margin);
		fdlCommit.top = new FormAttachment(wlId, margin );
		wlCommit.setLayoutData(fdlCommit);

		wCommit = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wCommit);
		wCommit.addModifyListener(lsMod);
		FormData fdCommit = new FormData();
		fdCommit.left = new FormAttachment(middle, 0);
		fdCommit.top = new FormAttachment(wlId, margin );
		fdCommit.right = new FormAttachment(100, 0);
		wCommit.setLayoutData(fdCommit);

		// Output Id line...
		wlIdOutput = new Label(shell, SWT.RIGHT);
		wlIdOutput.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.OutputId.Label")); //$NON-NLS-1$
 		props.setLook(wlIdOutput);
		FormData fdlIdOutput = new FormData();
		fdlIdOutput.left = new FormAttachment(0, 0);
		fdlIdOutput.right = new FormAttachment(middle, -margin);
		fdlIdOutput.top = new FormAttachment(wlCommit, margin );
		wlIdOutput.setLayoutData(fdlIdOutput);

		wIdOutput = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wIdOutput);
		wIdOutput.addModifyListener(lsMod);
		FormData fdIdOutput = new FormData();
		fdIdOutput.left = new FormAttachment(middle, 0);
		fdIdOutput.top = new FormAttachment(wlCommit, margin );
		fdIdOutput.right = new FormAttachment(100, 0);
		wIdOutput.setLayoutData(fdIdOutput);

	
	    /*************************************************
        // LOCALE SETTINGS
		*************************************************/
		
        // Language line...
		wlLang = new Label(shell, SWT.RIGHT);
		wlLang.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.Language.Label")); //$NON-NLS-1$
 		props.setLook(wlLang);
		FormData fdlLang = new FormData();
		fdlLang.left = new FormAttachment(0, 0);
		fdlLang.right = new FormAttachment(middle, -margin);
		fdlLang.top = new FormAttachment(wIdOutput, margin );
		wlLang.setLayoutData(fdlLang);

		wLang = new TextVar(transMeta,shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wLang);
 		wLang.addModifyListener(lsMod);
		FormData fdLang = new FormData();
		fdLang.left = new FormAttachment(middle, 0);
		fdLang.top = new FormAttachment(wIdOutput, margin );
		fdLang.right = new FormAttachment(100, 0);
		wLang.setLayoutData(fdLang);
		
        // Country line...
		wlCountry = new Label(shell, SWT.RIGHT);
		wlCountry.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.Country.Label")); //$NON-NLS-1$
 		props.setLook(wlCountry);
		FormData fdlCountry = new FormData();
		fdlCountry.left = new FormAttachment(0, 0);
		fdlCountry.right = new FormAttachment(middle, -margin);
		fdlCountry.top = new FormAttachment(wLang, margin );
		wlCountry.setLayoutData(fdlCountry);

		wCountry = new TextVar(transMeta,shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wCountry);
 		wCountry.addModifyListener(lsMod);
		FormData fdCountry = new FormData();
		fdCountry.left = new FormAttachment(middle, 0);
		fdCountry.top = new FormAttachment(wLang, margin );
		fdCountry.right = new FormAttachment(100, 0);
		wCountry.setLayoutData(fdCountry);

	    /*************************************************
        // CHECKBOX SETTINGS
		*************************************************/
		
        // Year null line...
		wlYearCheck = new Label(shell, SWT.RIGHT);
		wlYearCheck.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.Year.Check.Label")); //$NON-NLS-1$
 		props.setLook(wlYearCheck);
		FormData fdlYearCheck = new FormData();
		fdlYearCheck.left = new FormAttachment(0, 0);
		fdlYearCheck.right = new FormAttachment(middle, -margin);
		fdlYearCheck.top = new FormAttachment(wCountry, margin );
		wlYearCheck.setLayoutData(fdlYearCheck);

		wYearCheck = new Button(shell, SWT.CHECK);;
 		props.setLook(wYearCheck);
 		wYearCheck.addSelectionListener(lsSelection);
		FormData fdYearCheck = new FormData();
		fdYearCheck.left = new FormAttachment(middle, 0);
		fdYearCheck.top = new FormAttachment(wCountry, margin );
		fdYearCheck.right = new FormAttachment(100, 0);
		wYearCheck.setLayoutData(fdYearCheck);

        // Only lookup...
		wlOnlyLookup = new Label(shell, SWT.RIGHT);
		wlOnlyLookup.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.OnlyLookup.Check.Label")); //$NON-NLS-1$
 		props.setLook(wlOnlyLookup);
		FormData fdlOnlyLookup = new FormData();
		fdlOnlyLookup.left = new FormAttachment(0, 0);
		fdlOnlyLookup.right = new FormAttachment(middle, -margin);
		fdlOnlyLookup.top = new FormAttachment(wYearCheck, margin );
		wlOnlyLookup.setLayoutData(fdlOnlyLookup);

		wOnlyLookup = new Button(shell, SWT.CHECK);;
 		props.setLook(wOnlyLookup);
 		wOnlyLookup.addSelectionListener(lsSelection);
		FormData fdOnlyLookup = new FormData();
		fdOnlyLookup.left = new FormAttachment(middle, 0);
		fdOnlyLookup.top = new FormAttachment(wYearCheck, margin );
		fdOnlyLookup.right = new FormAttachment(100, 0);
		wOnlyLookup.setLayoutData(fdOnlyLookup);
		
	    /*************************************************
        // TABLE FIELDS
		*************************************************/

        // Table with fields to sort and sort direction
		wlFields=new Label(shell, SWT.NONE);
		wlFields.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.Fields.Label"));
 		props.setLook(wlFields);
		FormData fdlFields=new FormData();
		fdlFields.left = new FormAttachment(0, 0);
		fdlFields.top  = new FormAttachment(wOnlyLookup, margin);
		wlFields.setLayoutData(fdlFields);
		
		final int FieldsRows=input.getDateTypes().length;
		
		colinf=new ColumnInfo[] {
				new ColumnInfo(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.DateField.Column"),  ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Year"), BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Month"), BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Week"), BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Day"), BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Bimonthly"), BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Quarter"), BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.TableField.Semester") }, true ),
				new ColumnInfo(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.StreamField.Column"),  ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false),
			};
		
		wFields=new TableView(transMeta, shell, 
							  SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, 
							  colinf, 
							  FieldsRows,  
							  lsMod,
							  props
							  );

		FormData fdFields=new FormData();
		fdFields.left  = new FormAttachment(0, 0);
		fdFields.top   = new FormAttachment(wlFields, margin);
		fdFields.right = new FormAttachment(100, 0);
		wFields.setLayoutData(fdFields);

        // Search the fields in the background
        final Runnable runnable = new Runnable()
        {
            public void run()
            {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta!=null)
                {
                    try
                    {
                    	RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);
                       
                        // Remember these fields...
                        for (int i=0;i<row.size();i++)
                        {
                            inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                        }
                        setComboBoxes();
                    }
                    catch(KettleException e)
                    {
                    	logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                    }
                }
            }
        };
        new Thread(runnable).start();

		
		// OK and cancel buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); 
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); 
		
		// SQL Button
		wCreate=new Button(shell, SWT.PUSH);
		wCreate.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.SQL.Button"));


		BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel, wCreate }, margin, wFields);

		
		// Add listeners
		lsCancel = new Listener() { public void handleEvent(Event e) { cancel(); } };
		lsOK = new Listener() { public void handleEvent(Event e) { ok(); } };
		lsCreate = new Listener() { public void handleEvent(Event e) { create();     } };

		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);
		wCreate.addListener(SWT.Selection, lsCreate);

		lsDef = new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };

		wStepname.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } });
		
		wbSchema.addSelectionListener
		(
			new SelectionAdapter()
			{
				public void widgetSelected(SelectionEvent e) 
				{
					getSchemaNames();
				}
			}
		);
		
		wbTable.addSelectionListener
		(
			new SelectionAdapter()
			{
				public void widgetSelected(SelectionEvent e)
				{
					getTableName();
				}
			}
		);
		
		// Set the shell size, based upon previous time...
		setSize();

		getData();
		input.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		
		return stepname;
	}
	
	// Read data and place it in the dialog
	public void getData() {
		wStepname.selectAll();
		if (input.getDatabaseMeta() != null) 
			wConnection.setText( input.getDatabaseMeta().getName() );
		
		if (input.getSchemaName() != null)
			wSchema.setText(input.getSchemaName());
		
		if (input.getTableName() != null)
			wTable.setText(input.getTableName());	
		
		if (input.getTechKey() != null)
			wId.setText(input.getTechKey());

		if (input.getCommitSize() != null)
			wCommit.setText(""+input.getCommitSize());

		if (input.getOutputField() != null)
			wIdOutput.setText(input.getOutputField());

		if (input.getLocaleLanguage() != null)
			wLang.setText(input.getLocaleLanguage());

		if (input.getLocaleCountry() != null)
			wCountry.setText(input.getLocaleCountry());

		wYearCheck.setSelection(input.isYearNullActive());

		wOnlyLookup.setSelection(input.isOnlyLookupActive());

		Table table = wFields.table;
		if (input.getDateTypes().length>0) table.removeAll();
		for (int i=0;i<input.getDateTypes().length;i++)
		{
			TableItem ti = new TableItem(table, SWT.NONE);
			ti.setText(0, ""+(i+1));
			ti.setText(1, input.getDateTypes()[i]);
			ti.setText(2, input.getStreamFields()[i]);
		}

        wFields.setRowNums();
		wFields.optWidth(true);


	}
	
	private void cancel() {
		stepname = null;
		input.setChanged(changed);
		dispose();
	}
	
	// let the meta know about the entered data
	private void ok() {
		
		if (Const.isEmpty(wStepname.getText())) return;
		
		stepname = wStepname.getText(); // return value
		
		if (ci == null)
		{
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR );
			mb.setMessage(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.SelectValidConnection"));
			mb.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Dialog.DialogCaptionError"));
			mb.open();
			return;
		}

		input.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
		input.setSchemaName(wSchema.getText());
		input.setTableName(wTable.getText());
		input.setTechKey(wId.getText());
		input.setCommitSize( Const.toInt(wCommit.getText(), 0) );
		input.setOutputField(wIdOutput.getText());
		input.setLocaleLanguage(wLang.getText());
		input.setLocaleCountry(wCountry.getText());
		input.setYearNullActive(wYearCheck.getSelection());
		input.setOnlyLookupActive(wOnlyLookup.getSelection());
		
		int nrfields = wFields.nrNonEmpty();
		input.allocate(nrfields);
		for (int i=0;i<nrfields;i++)
		{
			TableItem ti = wFields.getNonEmpty(i);
			input.getDateTypes()[i] = ti.getText(1);
			input.getStreamFields()[i] = ti.getText(2);
		}


		dispose();
	}
	
	protected void setComboBoxes() {

		// Something was changed in the row.
        final Map<String, Integer> fields = new HashMap<String, Integer>();
        
        // Add the currentMeta fields...
        fields.putAll(inputFields);
        
        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);

        String fieldNames[] = (String[]) entries.toArray(new String[entries.size()]);

        Const.sortStrings(fieldNames);
        colinf[1].setComboValues(fieldNames);
    }

	private void getSchemaNames() {
		
		DatabaseMeta databaseMeta = transMeta.findDatabase(wConnection.getText());
		if (databaseMeta!=null)
		{
			Database database = new Database(loggingObject, databaseMeta);
			try
			{
				database.connect();
				String schemas[] = database.getSchemas();
				
				if (null != schemas && schemas.length>0) {
					schemas=Const.sortStrings(schemas);	
					EnterSelectionDialog dialog = new EnterSelectionDialog(shell, schemas, 
							BaseMessages.getString(PKG,"LookupTimeDimension.Meta.AvailableSchemas.Title",wConnection.getText()), 
							BaseMessages.getString(PKG,"LookupTimeDimension.Meta.AvailableSchemas.Message",wConnection.getText()));
					String d=dialog.open();
					if (d!=null) 
					{
						wSchema.setText(Const.NVL(d.toString(), ""));
					}

				}else
				{
					MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR );
					mb.setMessage(BaseMessages.getString(PKG,"LookupTimeDimension.Meta.NoSchema.Error"));
					mb.setText(BaseMessages.getString(PKG,"LookupTimeDimension.Meta.GetSchemas.Error"));
					mb.open(); 
				}
			}
			catch(Exception e)
			{
				new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"), 
						BaseMessages.getString(PKG,"LookupTimeDimension.Meta.ErrorGettingSchemas"), e);
			}
			finally
			{
				if(database!=null) 
				{
					database.disconnect();
					database=null;
				}
			}
		}
	}
	
	private void getTableName() {
		
		DatabaseMeta inf = null;
		// New class: SelectTableDialog
		int connr = wConnection.getSelectionIndex();
		if (connr >= 0) inf = transMeta.getDatabase(connr);

		if (inf != null)
		{
			logDebug(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.Log.LookingAtConnection", inf.toString()));

			DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
			std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
			
			if (std.open())
			{
                wSchema.setText(Const.NVL(std.getSchemaName(), ""));
				wTable.setText(Const.NVL(std.getTableName(), ""));
			}
		}
		else
		{
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.ConnectionError2.DialogMessage"));
			mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
			mb.open();
		}
	}	
	
	private void getInfo(LookupTimeDimensionMeta info) {

		info.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
		info.setSchemaName(wSchema.getText());
		info.setTableName(wTable.getText());
		info.setTechKey(wId.getText());
		info.setOutputField(wIdOutput.getText());
		info.setLocaleLanguage(wLang.getText());
		info.setLocaleCountry(wCountry.getText());
		info.setYearNullActive(wYearCheck.getSelection());
		info.setOnlyLookupActive(wOnlyLookup.getSelection());
		
		int nrfields = wFields.nrNonEmpty();
		info.allocate(nrfields);
		for (int i=0;i<nrfields;i++)
		{
			TableItem ti = wFields.getNonEmpty(i);
			info.getDateTypes()[i] = ti.getText(1);
			info.getStreamFields()[i] = ti.getText(2);
		}
		
		info.setCommitSize( Const.toInt(wCommit.getText(), 0) );
		
	}

	/** 
	 *  Generate code for create table. Conversions done by database.
	 */
	private void create() {
		
		LookupTimeDimensionMeta info = new LookupTimeDimensionMeta();
		getInfo(info);
		String name = stepname;  // new name might not yet be linked to other steps!
		StepMeta stepMeta = new StepMeta(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.StepMeta.Title"), name, info);

		//RowMetaInterface prev = getFieldsSql();
		RowMetaInterface prev = ((LookupTimeDimensionData) input.getStepData()).metaAttrs;
		
		SQLStatement sql = info.getSQLStatements(transMeta, stepMeta, prev);
		
		if (!sql.hasError())
		{
			if (sql.hasSQL())
			{
				SQLEditor sqledit = new SQLEditor(transMeta, shell, SWT.NONE, info.getDatabaseMeta(), transMeta.getDbCache(), sql.getSQL());
				sqledit.open();
			}
			else
			{
				MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION );
				mb.setMessage(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.NoSQLNeeds.DialogMessage"));
				mb.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.NoSQLNeeds.DialogTitle"));
				mb.open();
			}
		}
		else
		{
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR );
			mb.setMessage(sql.getError());
			mb.setText(BaseMessages.getString(PKG, "LookupTimeDimension.Meta.SQLError.DialogTitle")); //$NON-NLS-1$
			mb.open();
		}
		
	}

}
