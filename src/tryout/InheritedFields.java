package tryout;

public class InheritedFields {

	public static abstract class Parent {
		protected String s;
	}

	public static class ChildA extends Parent {
		public ChildA() {
			s = "aa";
		}
	}

	public static class ChildB extends Parent {
		String s = "b";
	}

	public static void main(String[] args) {
		Parent p = new ChildA();
		System.out.println(p.s);
		p = new ChildB();
		System.out.println(p.s);
	}

}
