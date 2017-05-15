package com.example;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
import java.io.IOException;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.util.HashMap;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
public class Mihin_Patient
{
	private static long row_id = 0;
	static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
			String line = c.element();
			JSONArray indicationObject =null;
			String name = null, city = null, state = null, postal_code = null, bdate = null, gender = null, all_json = null, patient_id = null;
			JSONParser parser = new JSONParser();
			try {
				Object obj = parser.parse(line);
				JSONObject jsonObject = (JSONObject) obj;
				JSONArray resource = (JSONArray) jsonObject.get("resources");
				for (int i = 0; i < resource.size(); i++) {
					row_id = row_id +1;
					JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
					HashMap map  = (HashMap) jsonObject1.get("resource");
					all_json =String.valueOf(map);
					JSONArray FullnameArray  = (JSONArray) map.get("name");
    		 			JSONObject nameObject  = (JSONObject) parser.parse(FullnameArray.get(0).toString());
    					JSONArray nameArray = (JSONArray)(nameObject.get("given"));
					String t="";
    					for(int j=0;j<nameArray.size();j++)
					{
						if(j==0)
							t=String.valueOf(nameArray.get(j))+" ";
						else if(j==(nameArray.size()-1))
						t+=nameArray.get(j);
						else
						t=nameArray.get(j)+" ";	
					}
					name = t;
					if ( map.get("address") != null) {
    			  
       						JSONObject addressObject  = (JSONObject) parser.parse(((JSONArray) map.get("address")).get(0).toString());
        				
        					city = String.valueOf(addressObject.get("city"));
                     				state = String.valueOf(addressObject.get("state"));
                              			postal_code = String.valueOf(addressObject.get("postalCode"));
                                        
					}				
					bdate = String.valueOf(map.get("birthDate"));
					gender = String.valueOf(map.get("gender"));
					patient_id = String.valueOf(map.get("id"));
					TableRow row = new TableRow().set("name", name).set("city", city).set("state",state).set("postal_code",postal_code).set("bdate",bdate).set("gender",gender).set("all_json",all_json);
					c.output(row);
				}
			}
			catch(Exception e){
				e.printStackTrace(); 
				throw e;
			}
		}

	}; 
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.Read.named("Fetching to cloud").from("gs://mihin-data/PatientFormated.json")).apply(ParDo.named("Transforming from FHIR -> Table Format ").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
				.named("Pushing to BigQuerry")
				.to("healthcare-12:Mihin_Data_Sample.Mihin_Patient_Entry")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();
		/*
.apply(BigQueryIO.Write
      .named("Write")
      .to("healthcare-12:Mihin_Data_Sample.Encounter_Entry")
     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		 */
		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}
