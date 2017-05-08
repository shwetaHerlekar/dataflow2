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
public class Mihin_Encounter
{
	private static long row_id = 0;
	static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
			String line = c.element();
			JSONArray indicationObject =null;
			String patientId = null, startDate = null,startMonth,startYear,kind=null,e_id = null ;
			JSONParser parser = new JSONParser();
			try {
				Object obj = parser.parse(line);
				JSONObject jsonObject = (JSONObject) obj;
				JSONArray resource = (JSONArray) jsonObject.get("resources");
				for (int i = 0; i < resource.size(); i++) {
					row_id = row_id +1;
					JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
					HashMap map  = (HashMap) jsonObject1.get("resource");
					HashMap<String , JSONArray> map2  =  (HashMap<String, JSONArray>) jsonObject1.get("resource");
					JSONObject patientObj  =  (JSONObject) map.get("patient");
					String patient =  (String) patientObj.get("reference");
					patientId = patient.substring(patient.indexOf('/')+1);
					JSONObject periodObj  =  (JSONObject) map.get("period");
					startDate =  (String) periodObj.get("start");
					kind =  (String) map.get("class");
					e_id = (String) map.get("id"); 
					SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
					java.util.Date date = sdf1.parse(startDate);
					java.sql.Date sqlStartDate = new java.sql.Date(date.getTime());
					TableRow row = new TableRow().set("encounter_id", e_id).set("class", kind).set("patient_id",patientId).set("startDate",sqlStartDate);
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
		p.apply(TextIO.Read.from("gs://mihin-data/formatedEncounterEntry.json")).apply(ParDo.named("Loading to Bigtable").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
				.named("Write")
				.to("healthcare-12:Mihin_Data_Sample.Encounter_Entry")
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
