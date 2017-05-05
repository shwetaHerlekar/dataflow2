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
import java.text.SimpleDateFormat;
public class CitiesDataEntry
{
   private static long row_id = 0;
static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
  private static final long serialVersionUID = 1L;
  @Override
  public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
  	String line = c.element();
	  CSVParser csvParser = new CSVParser();
	String[] parts = csvParser.parseLine(line);
	String Year = parts[0] ;String State = parts[2];String Category = parts[6] ;String Measure = parts[8] ;String Data_Value = parts[12];
	String Low_confidence_Limit = parts[13];String High_confidence_Limit = parts[14] ;String Population = parts[17] ;String issue = parts[21];
	TableRow row = new TableRow().set("Year", Year).set("State", State).set("Category",Category)
	.set("Measure",Measure).set("Data_Value",Data_Value).set("Low_confidence_Limit",Low_confidence_Limit).set("High_confidence_Limit",High_confidence_Limit)
		.set("Population",Population).set("issue",issue);
   	c.output(row);
  }

}; 
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
 		p.apply(TextIO.Read.named("Fetching File from Cloud").from("gs://mihin-data/formatedEncounterEntry.json")).apply(ParDo.named("Processing File").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
      .named("Writeing to Big Querry")
      .to("healthcare-12:health_care_data.500_cities_local_data_for_better_health")
     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();

	}

}
