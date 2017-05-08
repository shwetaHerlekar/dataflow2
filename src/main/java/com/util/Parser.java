package com.util;
import com.opencsv.CSVParser;
import java.io.IOException;
import com.dao.*;
public class Parser {
	private CSVParser csvParser;
	public Parser() {
		// TODO Auto-generated constructor stub
		csvParser = new CSVParser();
	}
	public CitiesData getCityData(String csvData) throws IOException{
		String[] data = csvParser.parseLine(csvData);
		CitiesData cityDataObject = new CitiesData();
		int year = Integer.parseInt(data[0]) ;
		String state = data[2];
		String category = data[6] ;
		String measure = data[8] ;
		int data_value = Float.parseFloat(data[12]);
		String low_confidence_Limit = data[13];
		String high_confidence_Limit = data[14] ;
		double population = Double.parseDouble(data[17]) ;
		String issue = data[23];
		cityDataObject.setCategory(category);
		cityDataObject.setData_value(data_value);
		cityDataObject.setHigh_confidence_Limit(high_confidence_Limit);
		cityDataObject.setIssue(issue);
		cityDataObject.setLow_confidence_Limit(low_confidence_Limit);
		cityDataObject.setYear(year);
		cityDataObject.setMeasure(measure);
		cityDataObject.setPopulation(population);
		cityDataObject.setState(state);
		return cityDataObject;
	}
}
