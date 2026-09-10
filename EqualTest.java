import java.util.Objects;

public class EqualTest {

    static class RedisObject {
        enum Type { STRING, LIST, HASH, SET }
        final Type type;
        final Object payLoad;

        public RedisObject(Type type, Object payLoad) {
            this.type = type;
            this.payLoad = payLoad;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            RedisObject obj = (RedisObject) other;
            return type == obj.type && Objects.equals(payLoad, obj.payLoad);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, payLoad);
        }
    }

    public static void main(String[] args) {
        RedisObject a = new RedisObject(RedisObject.Type.STRING, "abhinay");
        RedisObject b = new RedisObject(RedisObject.Type.STRING, "abhinay");
        RedisObject c = new RedisObject(RedisObject.Type.STRING, "different");
        RedisObject d = new RedisObject(RedisObject.Type.LIST, "abhinay");

        System.out.println("a == b (reference): " + (a == b)); // false because they are pointing to different objects.
        System.out.println("a.equals(b) (same type, same payload): " + a.equals(b));  //true because they are pointing to diff object but later they are having same type and payload.
        System.out.println("a.equals(c) (same type, different payload): " + a.equals(c)); // false bec diff payload if that was same it would be true.
        System.out.println("a.equals(d) (different type, same payload): " + a.equals(d)); // false ,diff types.
        System.out.println("a.equals(\"abhinay\") (compared against a plain String): " + a.equals("abhinay")); // false , that is a string getClass() != other.getClass().
        System.out.println("a.equals(null): " + a.equals(null)); //false because getClass() != other.getClass()
        System.out.println("x.euqla(a) or null . equal(a)" + Objects.equals(null,a)); // false because they are diff and other == null.

        RedisObject nullPayloadA = new RedisObject(RedisObject.Type.STRING, null);
        RedisObject nullPayloadB = new RedisObject(RedisObject.Type.STRING, null);
        System.out.println("both null payloads, same type: " + nullPayloadA.equals(nullPayloadB)); // true bez they are same type and payload.
    }
}
