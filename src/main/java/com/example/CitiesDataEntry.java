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
public class Mihin_Encounter
{
   private static long row_id = 0;
static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
  private static final long serialVersionUID = 1L;
  @Override
  public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
  	String line = c.element();
   	c.output(line);
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
      .to("healthcare-12:health_care_data.500_cities_local_data_for_better_health")
     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();

	}

}
