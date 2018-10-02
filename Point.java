package com.paritosh.learning.hadoop;


public class Point {
	int x;
	int y;
	public Point() {
		x = getRandomValueInRange(1,10000);
		y = getRandomValueInRange(1,10000);
	}
	
	public Point(int x, int y) {
		x = this.x ;
		y = this.y ;
	}
	
	static int getRandomValueInRange(int min, int max) {
		return (int) (min + (Math.random() * (max - min)));
	}
	
	@Override
	public String toString() {
		
		return "P<" + String.valueOf(x) + "," + String.valueOf(y) + ">";
	}
}