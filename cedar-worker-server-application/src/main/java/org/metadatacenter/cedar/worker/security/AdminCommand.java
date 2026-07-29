package org.metadatacenter.cedar.worker.security;

import org.metadatacenter.exception.CedarException;
import org.metadatacenter.rest.context.CedarRequestContext;
import org.metadatacenter.server.security.model.auth.CedarPermission;

import static org.metadatacenter.rest.assertion.GenericAssertions.LoggedIn;

/**
 * The worker server's administrative commands and the permission each one requires, with the
 * authorization gate itself. This mirrors the resource server's {@code AdminCommand}: rather than each
 * command carrying its own inline {@code c.must(c.user()).be(LoggedIn); c.must(c.user()).have(...)}
 * pair, the command is bound to its permission in one table and the assertion runs from one place, so a
 * gate cannot be dropped or pointed at the wrong permission unnoticed.
 *
 * <p>There is one such command today ({@code regenerate-inclusion-subgraph}); the enum is the seam that
 * keeps the gate uniform as more are added. {@link #enforce} preserves the exact behaviour of the former
 * inline check, so a denial still maps to 401 (anonymous) or 403 (logged in without the permission),
 * pinned by {@code AdminCommandAuthorizationMatrixTest}.
 */
public enum AdminCommand {

  REGENERATE_INCLUSION_SUBGRAPH(CedarPermission.INCLUSION_SUBGRAPH_RECREATE);

  private final CedarPermission permission;

  AdminCommand(CedarPermission permission) {
    this.permission = permission;
  }

  /**
   * Assert that the caller of this command is logged in and holds the command's permission. A denial
   * throws and maps to 401 (not logged in) or 403 (logged in without the permission).
   */
  public void enforce(CedarRequestContext c) throws CedarException {
    c.must(c.user()).be(LoggedIn);
    c.must(c.user()).have(permission);
  }
}
