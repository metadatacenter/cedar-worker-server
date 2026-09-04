package org.metadatacenter.worker;

import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.config.environment.CedarEnvironmentVariable;

/**
 * How the log aggregation jobs read their settings.
 *
 * <p>Each of the three jobs carried a private {@code env(String, String)} calling
 * {@link System#getenv} — three identical copies, and all three outside the configuration model.
 * That was not an oversight so much as a consequence: declaring a variable used to make it
 * mandatory, and a batch size that stops the worker from booting is worse than one left unset. So
 * fifteen settings lived only in the source of whatever read them, invisible to the boot-time
 * sandbox report and to the monitoring server's environment page alike.
 *
 * <p>They are declared now, as optional, and read from the same sandbox every other CEDAR setting
 * comes from. Two things follow. A setting names a {@link CedarEnvironmentVariable} rather than a
 * string, so a rename is a compile error instead of a job that silently reverts to its default. And
 * a test can redirect these through {@code CedarEnvironmentSource} like anything else, where
 * {@code System.getenv} could not be redirected at all.
 */
final class JobEnvironment {

  private JobEnvironment() {
  }

  /**
   * The variable's value, or {@code dflt} where the environment does not supply one.
   *
   * <p>Empty counts as unset. A variable exported as the empty string is how a shell says "I did not
   * really mean to set this", and parsing "" as a batch size helps nobody.
   */
  static String read(CedarEnvironmentVariable variable, String dflt) {
    String value = CedarConfig.getInstanceEnvironment().get(variable.getName());
    return (value == null || value.isEmpty()) ? dflt : value;
  }
}
