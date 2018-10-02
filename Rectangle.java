package com.paritosh.learning.hadoop;

public class Rectangle {
	int bottomX;
	int bottomY;
	int height;
	int width;
	
	Rectangle() {
		bottomX = getRandomValueInRange(1, 10000);
		bottomY = getRandomValueInRange(1, 10000);
		height  = getRandomValueInRange(1, 20);
		width	= getRandomValueInRange(1, 5);
	}
	
	static int getRandomValueInRange(int min, int max) {
		return (int) (min + (Math.random() * (max - min)));
	}
}

