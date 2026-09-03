package dev.actorframework.testkit;

import dev.actorframework.core.ActorSystem;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * A JUnit 5 extension providing an isolated {@link ActorSystem} per test (TASK-110: "isolated actor
 * systems for tests"). The system is created lazily on first use and closed automatically after the
 * test, so tests never leak actor systems or their threads into one another.
 *
 * <pre>{@code
 * @ExtendWith(ActorSystemExtension.class)
 * class MyActorTest {
 *     @Test
 *     void greets(ActorSystem system) {
 *         ...
 *     }
 * }
 * }</pre>
 */
public final class ActorSystemExtension implements ParameterResolver, AfterEachCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(ActorSystemExtension.class);
  private static final String STORE_KEY = "actorSystem";

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == ActorSystem.class;
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(STORE_KEY, key -> ActorSystem.start(), ActorSystem.class);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    ActorSystem system = context.getStore(NAMESPACE).remove(STORE_KEY, ActorSystem.class);
    if (system != null) {
      system.close();
    }
  }
}
