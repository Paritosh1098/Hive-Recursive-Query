package com.paritosh.learning.hadoop;

import java.io.FileWriter;
import java.io.IOException;

public class CreateDataSetProject2 {

	
	static int getRandomValueInRange(int min, int max) {
		return (int) (min + (Math.random() * (max - min)));
	}

	
//	public static class Point {
//		int x;
//		int y;
//		public Point() {
//			x = getRandomValueInRange(1,10000);
//			y = getRandomValueInRange(1,10000);
//		}
//		
//		public Point(int x, int y) {
//			x = this.x ;
//			y = this.y ;
//		}
//		
//		static int getRandomValueInRange(int min, int max) {
//			return (int) (min + (Math.random() * (max - min)));
//		}
//		
//		@Override
//		public String toString() {
//			
//			return "P<" + String.valueOf(x) + "," + String.valueOf(y) + ">";
//		}
//	}
//	
//	public static class Rectangle {
//		int bottomX;
//		int bottomY;
//		int height;
//		int width;
//		
//		Rectangle() {
//			bottomX = getRandomValueInRange(1, 10000);
//			bottomY = getRandomValueInRange(1, 10000);
//			height  = getRandomValueInRange(1, 20);
//			width	= getRandomValueInRange(1, 5);
//		}
//	}
	
	public static void main(String[] args) throws IOException {
		
		FileWriter pointsWriter = new FileWriter("points.csv",false);
		FileWriter rectangleWriter = new FileWriter("rectangles.csv",false);
		
		for(int j=0; j<1500; j++) {
			
			for(int i=0; i<10000; i++) {
				Point point = new Point();
				Rectangle rectangle = new Rectangle();
				
				pointsWriter.write(point.x + "," + point.y + "\n") ;
				rectangleWriter.write(rectangle.bottomX + "," + rectangle.bottomY + "," + rectangle.height + "," + rectangle.width + "\n");
			}
		}
		pointsWriter.close();
		rectangleWriter.close();
	}
}
