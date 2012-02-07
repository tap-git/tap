package tap.core.io;

public enum Types {
	STRING(1), BOOL(2), INT(3), LONG(4);
	
	private final int value;
	private Types(int value) {
		this.value = value;
	}
	
	public int asc() {
		return value;
	}
	
	public int desc() {
		return 255 - value;
	}
	
	public int value(SortOrder order) {
		return order == SortOrder.ASCENDING ? asc() : desc();
	}
}
