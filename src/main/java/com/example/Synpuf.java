package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
import java.io.IOException;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.dataflow.sdk.io.*;
public class Synpuf
{
	
 	static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
  		private static final long serialVersionUID = 1L;
  		@Override
 		 public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
  			String line = c.element();
		 	CSVParser csvParser = new CSVParser();
 			String[] parts = csvParser.parseLine(line);
 			TableRow row = new TableRow();
 			row.set("id" ,parts[0] );
 			row.set("death_date" ,parts[2]  );
 			row.set("sex" , parts[3] );
 			row.set("esrdi" , parts[5] );
 			row.set("state_code" ,parts[6]  );
 			row.set("anual_mediclaim_inpatient" ,parts[12]  );
 			row.set("anual_mediclaim_outpatient" ,parts[13]  );
 			row.set("SP_ALZHDMTA" , parts[14] );
 			row.set("SP_CHF" , parts[15] );
 			row.set("SP_CHRNKIDN" , parts[16] );
 			row.set("SP_CNCR" , parts[17] );
 			row.set("SP_COPD" , parts[18] );
 			row.set("SP_DEPRESSN" , parts[19] );
 			row.set("SP_DIABETES" , parts[20]  );
 			row.set("SP_ISCHMCHT" , parts[21] );
 			row.set("SP_OSTEOPRS" ,parts[22]  );
 			row.set("SP_RA_OA1" , parts[23]  );
 			row.set("SP_STRKETIA" , parts[27] );
   			c.output(row);
  }
};
		public static void main(String[] args) 
		{
			DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
			options.setRunner(BlockingDataflowPipelineRunner.class);
			options.setProject("healthcare-12");
			options.setStagingLocation("gs://synpuf_data/staging1");
			Pipeline p = Pipeline.create(options);
 			p.apply(TextIO.Read.named("Reading Synpuf data ").from("gs://synpuf_data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv")).apply(ParDo.named("Loading to BigTable").of(MUTATION_TRANSFORM)).apply(BigQueryIO.Write.named("Write").to("healthcare-12:synpuf_data.Beneficiary_summary").withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
			p.run();

		}

}
