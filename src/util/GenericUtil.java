package util;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericUtil {

	  public static boolean isEmpty(Object o) {
	    if (o == null) return true;
	    if (o instanceof Map) return isEmpty((Map) o);
	    if (o instanceof List) return isEmpty((List) o);
	    if (o instanceof String) return isEmpty((String) o);
	    if (o instanceof Set) return isEmpty((Set) o);
	    return false;
	  }

	  public static boolean isEmpty(Map m) {
	    return m == null || m.isEmpty();
	  }

	  public static boolean isEmpty(List l) {
	    return l == null || l.isEmpty();
	  }

	  public static boolean isEmpty(String s) {
	    return s == null || s.length() == 0;
	  }

	  public static boolean isEmpty(Set s) {
	    return s == null || s.isEmpty();
	  }
}
