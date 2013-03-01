package au.com.aapt.awp.util.fusion;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.fusiontables.Fusiontables;
import com.google.api.services.fusiontables.FusiontablesScopes;
import com.google.api.services.fusiontables.model.FusiontablesImport;
import com.google.api.services.fusiontables.model.Sqlresponse;

public class FusionTableImporter {

	private static Log logger = LogFactory.getLog(FusionTableImporter.class);
	
	private static final String APPLICATION_NAME = "API Project";

	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	private static Fusiontables fusiontables;
	
	private static String SERVICE_ACCOUNT_ID;
	
	private static String PRIVATE_KEY_STORE_PATH;
	
	private static String TARGET_TABLE_ID;
	
	private static String DATA_FILE_PATH;
	
	private static String DATA_FILE_DELIMINATOR;

	private static void loadProperty(){
		Properties prop = new Properties();
		ClassLoader loader = Thread.currentThread().getContextClassLoader();           
		InputStream stream = loader.getResourceAsStream("config.properties");
		try {			
			prop.load(stream);
		} catch (IOException e) {
			logger.fatal("Fail to load config.properties file", e);
		}
		SERVICE_ACCOUNT_ID = prop.getProperty("oauth.service.account.id");
		PRIVATE_KEY_STORE_PATH = prop.getProperty("oauth.private.key.file.path");
		TARGET_TABLE_ID = prop.getProperty("target.fusion.table.id");
		DATA_FILE_PATH = prop.getProperty("data.file.path");
		DATA_FILE_DELIMINATOR = prop.getProperty("data.file.deliminator");
	}
	
	private static Credential authorizeWithServiceAccount() throws Exception{
		GoogleCredential credential;
		try {
			credential = new GoogleCredential.Builder().setTransport(HTTP_TRANSPORT)
			.setJsonFactory(JSON_FACTORY)
			.setServiceAccountId(SERVICE_ACCOUNT_ID)
			.setServiceAccountScopes(FusiontablesScopes.FUSIONTABLES)
			.setServiceAccountPrivateKeyFromP12File(new File(PRIVATE_KEY_STORE_PATH))
			.build();
		} catch (GeneralSecurityException e) {
			logger.fatal("Fail to authorize with Google API. Detail", e);
			throw e;
		} catch (IOException e) {
			logger.fatal("Fail to load private key file. Detail", e);
			throw e;
		}
		return credential;
	}

	private static void setupFusionTable(Credential credential){
		fusiontables = new Fusiontables.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME).build();
	}
	
	private static void cleanUpExistingTable(){
		try {
			Sqlresponse deleteResponse = fusiontables.query().sql("DELETE FROM "+TARGET_TABLE_ID).execute();
			logger.info("Clean up fusion table with ID "+TARGET_TABLE_ID+" resposne: "+deleteResponse.toPrettyString());
		} catch (IOException e) {
			logger.error("Fail to clean up existing fusion table "+TARGET_TABLE_ID, e);
		}
	}
	
	private static void uploadNewDataToTable(){
		FileContent content = new FileContent("application/octet-stream", new File(DATA_FILE_PATH));
        try {
			FusiontablesImport importResponse = fusiontables.table().importRows(TARGET_TABLE_ID, content).setDelimiter(DATA_FILE_DELIMINATOR).execute();
			logger.info("Upload data to fusion table with ID "+TARGET_TABLE_ID+" resposne: "+importResponse.toPrettyString());
        } catch (IOException e) {
			logger.error("Fail to upload data to fusion table with ID "+TARGET_TABLE_ID, e);
		}
	}
	
	public static void main(String[] args) throws Exception{
		loadProperty();
		
		setupFusionTable(authorizeWithServiceAccount());
		cleanUpExistingTable();   
		uploadNewDataToTable();
	}

}
