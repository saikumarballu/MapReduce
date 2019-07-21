
public class ShapeTest {
	public static void main(String str[]){
		Shape rect=new Rectangle(0,20);
		double areaOfRectangle=0;
		try{
			areaOfRectangle=rect.getArea();
		}catch(Exception e){
			e.printStackTrace();
		}
		System.out.println("The area of Rectangle is :"+areaOfRectangle);
		Shape circle=new Circle(5);
		double areaOfCircle=0;
		try{
			areaOfCircle=circle.getArea();
		}catch(Exception e){
			e.printStackTrace();
		}
		System.out.println("The area of Circle is :"+areaOfCircle);
	}
}
