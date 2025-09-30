package TokenizerConvert;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

public class Utils {
    public static BasicCredentialsProvider basicAuth(String user, String password) {
        BasicCredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));
        return provider;
    }

    public static Writer mapToJson(Map<String, Object> map) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            StringWriter writer = new StringWriter();
            mapper.writeValue(writer, map);
            return writer;
        } catch (Exception e) {
            throw new RuntimeException("mapToJson error: " + e.getMessage(), e);
        }
    }
}