
public class StringTest {
public static void main(String str[]){
	String str1=new String("test");
	int i=100000;
	System.out.println("The hashcode is :"+str1.hashCode());
	str1=str1+"test2";
	System.out.println("The hashcode after modification is :"+str1.hashCode());
	StringBuffer strBuf=new StringBuffer("test");
	System.out.println("The hashcode of initial string buffer is :"+strBuf.hashCode());
	strBuf.append("test1");
	System.out.println("The hashcode of final string buffer is :"+strBuf.hashCode());
	System.out.println("The hashcode of final int :"+new Integer(1000).hashCode());		

}
}
