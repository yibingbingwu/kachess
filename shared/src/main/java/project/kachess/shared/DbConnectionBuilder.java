package project.kachess.shared;

public class DbConnectionBuilder {
  private String host;
  private String user;
  private String port;
  private String schema;
  private String password;
  private boolean useSsl = false;
  private String envPrefix = "";

  private static final String[] BASE_KEYS = {"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PSWD"};

  private DbConnectionBuilder() {
  }

  public DbConnectionBuilder(String envKeyPrefix) {
    this.host = System.getenv(envKeyPrefix + BASE_KEYS[0]);
    this.user = System.getenv(envKeyPrefix + BASE_KEYS[3]);
    this.port = System.getenv(envKeyPrefix + BASE_KEYS[1]);
    this.schema = System.getenv(envKeyPrefix + BASE_KEYS[2]);
    this.password = System.getenv(envKeyPrefix + BASE_KEYS[4]);
  }

  public void setSchema(String newDb) {
    schema = newDb;
  }

  public void setUseSsl(boolean tf) {
    useSsl = tf;
  }

  public String getConnUrl(String defUrl) {
    if (envPrefix == null || host == null || user == null) {
      return defUrl;
    }

    StringBuilder retval =
        new StringBuilder("jdbc:mysql://").append(host);

    if (port != null) {
      retval.append(':').append(port);
    }

    retval.append('/');

    if (schema != null) {
      retval.append(schema);
    }

    retval.append("?user=").append(user);

    if (password != null) {
      retval.append("&password=" + password);
    }

    retval.append("&useSSL=" + useSsl);

    return retval.toString();
  }

  public String getSchema() {
    return schema;
  }
}
