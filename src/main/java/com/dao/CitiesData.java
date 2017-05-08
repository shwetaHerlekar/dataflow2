package com.dao;

public class CitiesData {
	private int year;
	private String state;
	private String category;
	private String measure;
	private String Low_confidence_Limit;
	private String High_confidence_Limit;
	private int data_value;
	private double population;
	private String issue;
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getMeasure() {
		return measure;
	}
	public void setMeasure(String measure) {
		this.measure = measure;
	}
	public String getLow_confidence_Limit() {
		return Low_confidence_Limit;
	}
	public void setLow_confidence_Limit(String low_confidence_Limit) {
		Low_confidence_Limit = low_confidence_Limit;
	}
	public String getHigh_confidence_Limit() {
		return High_confidence_Limit;
	}
	public void setHigh_confidence_Limit(String high_confidence_Limit) {
		High_confidence_Limit = high_confidence_Limit;
	}
	public int getData_value() {
		return data_value;
	}
	public void setData_value(int data_value) {
		this.data_value = data_value;
	}
	public double getPopulation() {
		return population;
	}
	public void setPopulation(double population) {
		this.population = population;
	}
	public String getIssue() {
		return issue;
	}
	public void setIssue(String issue) {
		this.issue = issue;
	}

}
