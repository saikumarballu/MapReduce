
public class Rectangle extends Shape {
	double length;
	double breadth;
	public Rectangle(){
		
	}
public Rectangle(double length, double breadth){
		this.length=length;
		this.breadth=breadth;
	}
	public double getArea() throws Exception{
		if(length==0 || breadth==0){
			throw new Exception("length or breadth cannot be ZERO");
		}
		return length*breadth;
	}
	
}
