
public class Circle extends Shape {
	double radius;
	
	public Circle(){
		
	}
public Circle(double radius){
		this.radius=radius;
		
	}
	public double getArea()throws Exception{
		if(radius==0){
			throw new Exception("the radius cannot be ZERO");
		}
		return Math.PI*radius*radius;
	}
	
}
