package io.joyrpc.spring.boot;

/*-
 * #%L
 * joyrpc
 * %%
 * Copyright (C) 2019 joyrpc.io
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.joyrpc.config.AbstractConsumerConfig;
import io.joyrpc.config.AbstractIdConfig;
import io.joyrpc.config.AbstractInterfaceConfig;
import io.joyrpc.config.ConsumerGroupConfig;
import io.joyrpc.spring.ConsumerBean;
import io.joyrpc.spring.ConsumerGroupBean;
import io.joyrpc.spring.Counter;
import io.joyrpc.spring.ProviderBean;
import io.joyrpc.spring.boot.annotation.AnnotationProvider;
import io.joyrpc.spring.schema.GlobalParameterDefinitionParser;
import io.joyrpc.util.Pair;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.ClassMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.beans.Introspector;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.joyrpc.spring.boot.Plugin.ANNOTATION_PROVIDER;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;
import static org.springframework.context.annotation.AnnotationConfigUtils.registerAnnotationConfigProcessors;
import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;
import static org.springframework.util.ClassUtils.getShortName;
import static org.springframework.util.ClassUtils.resolveClassName;
import static org.springframework.util.StringUtils.hasText;
import static org.springframework.util.StringUtils.isEmpty;

/**
 * 这是一个非常完整的 RPC 框架与 Spring Boot 集成的核心类 `RpcDefinitionPostProcessor`。它负责扫描注解、解析配置、注册 Bean 定义以及在 Bean 初始化后完成代理注入。
 *
 * 结合你之前提供的 `postProcessAfterInitialization` 方法片段和现在的完整类代码，我们可以更深入地理解这个类的工作原理。
 *
 *  1. 类的整体角色与职责
 *
 * `RpcDefinitionPostProcessor` 实现了三个关键接口，赋予了它在 Spring 容器启动不同阶段的介入能力：
 *
 * 1.  `BeanDefinitionRegistryPostProcessor`:
 *     *   核心阶段：在 Spring 容器启动的早期，所有常规 Bean 定义加载之后，但 Bean 实例化之前。
 *     *   职责：
 *         *   读取配置文件（`application.yml`）中的 RPC 配置。
 *         *   扫描指定包路径下的类，寻找 RPC 相关的注解（如 `@RpcService`, `@RpcReference` 等）。
 *         *   动态注册 `ConsumerBean`（消费者）、`ProviderBean`（提供者）、`Registry`（注册中心）等基础设施 Bean 的定义到 Spring 容器中。
 *
 * 2.  `BeanPostProcessor`:
 *     *   核心阶段：在 Bean 实例化并初始化前后。
 *     *   职责：
 *         *   `postProcessAfterInitialization`：这是你之前关注的方法。在业务 Bean（如 Controller、Service）初始化完成后，检查其字段或 setter 方法上是否有 Consumer 注解。如果有，从之前解析好的 `members` 缓存中获取对应的配置，创建（或获取已创建的）RPC 动态代理对象，并通过反射注入到 Bean 中。
 *
 * 3.  `BeanClassLoaderAware`:
 *     *   职责：获取当前应用的 `ClassLoader`，用于后续的类加载和反射操作。
 *
 * ---
 *
 *  2. 核心流程详解
 *
 *  第一阶段：注册与扫描 (`postProcessBeanDefinitionRegistry`)
 *
 * 这是整个类的入口，主要逻辑如下：
 *
 * 1.  加载配置：
 *
 *     this.rpcProperties = Binder.get(environment).bind(RPC_PREFIX, RpcProperties.class).orElseGet(RpcProperties::new);
 *
 *     将 `rpc.` 开头的配置（如注册中心地址、扫描包路径）绑定到 `RpcProperties` 对象。
 *
 * 2.  处理显式配置：
 *     遍历配置文件中定义的 Consumers、Groups 和 Providers，调用 `addConfig` 方法将它们放入内部的 Map（`consumers`, `providers` 等）中，并计算唯一的 Bean 名称。
 *
 * 3.  包扫描 (`processPackages`)：
 *     *   创建一个 `ClassPathBeanDefinitionScanner`。
 *     *   添加自定义的 `AnnotationFilter`（内部类），该过滤器会检查类是否包含 Provider 注解，或者字段/方法是否包含 Consumer 注解。
 *     *   对扫描到的候选组件（BeanDefinition），执行两个核心处理：
 *         *   `processConsumerAnnotation(definition)`: 解析该类定义中的 Consumer 注解（字段和方法），构建 `ConsumerBean` 对象并存入 `members` Map（键为 Field/Method，值为 Config）。注意：此时只是解析配置，并没有真正注入代理，因为 Bean 还没实例化。
 *         *   `processProviderAnnotation(definition, registry)`: 解析该类上的 Provider 注解，构建 `ProviderBean` 对象，并尝试推断服务接口，最后注册该服务实现的 Bean 定义。
 *
 * 4.  注册基础设施 Bean (`register`)：
 *     将收集到的所有 `ConsumerBean`、`ProviderBean`、注册中心配置、服务器配置等，正式注册为 Spring Bean 定义。这些 Bean 通常标记为 `ROLE_INFRASTRUCTURE`，表示它们是内部支撑组件。
 *
 *  第二阶段：实例化与注入 (`postProcessAfterInitialization`)
 *
 * 这是你之前询问的方法，现在结合上下文，其逻辑更加清晰：
 *
 *
 * @Override
 * public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
 *     //再次查找所有的Consumer注解，检查是否注入了。
 *     processConsumerAnnotation(bean.getClass(),
 *             (f, c) -> {
 *                 // 1. 根据字段 f 从 members Map 中查找预先解析好的配置
 *                 AbstractConsumerConfig<?> config = members.get(f);
 *                 if (config != null) {
 *                     // 2. 暴力反射，确保可以访问私有字段
 *                     ReflectionUtils.makeAccessible(f);
 *                     // 3. 关键点：config.proxy() 生成动态代理对象
 *                     //    并将其设置到当前 bean 的字段 f 中
 *                     ReflectionUtils.setField(f, bean, config.proxy());
 *                 }
 *             },
 *             (m, c) -> {
 *                 // 处理方法注入逻辑同上，通过 invokeMethod 调用 setter
 *                 AbstractConsumerConfig<?> config = members.get(m);
 *                 if (config != null) {
 *                     ReflectionUtils.invokeMethod(m, bean, config.proxy());
 *                 }
 *             });
 *     return bean;
 * }
 *
 *
 * 深度解析：
 * 1.  为什么是“再次查找”？
 *     *   在第一阶段（`postProcessBeanDefinitionRegistry`）中，`processConsumerAnnotation(BeanDefinition)` 已经扫描过类定义并填充了 `members` Map。
 *     *   在这里再次调用 `processConsumerAnnotation(Class)`，是为了在 Bean 实例化后，遍历其实际的 Class 对象，匹配 `members` Map 中的键，从而触发注入动作。
 *     *   这里的“查找”主要是为了遍历 Field 和 Method，而配置数据实际上取自第一阶段缓存的 `members` Map。
 *
 * 2.  `config.proxy()` 的奥秘：
 *     *   `config` 是 `AbstractConsumerConfig` 的实例（通常是 `ConsumerBean`）。
 *     *   `proxy()` 方法（在父类或 `ConsumerBean` 中实现）负责：
 *         *   检查是否已存在代理实例（单例模式）。
 *         *   如果不存在，使用工厂（如 `ProxyFactory`）基于 JDK 动态代理或 Javassist 创建代理对象。
 *         *   这个代理对象实现了服务接口，拦截方法调用，将其转换为 RPC 请求发送给远程服务提供者。
 *
 * 3.  注入时机：
 *     *   选择在 `AfterInitialization` 执行，确保：
 *         *   Bean 自身已完全初始化（`@PostConstruct` 等已执行）。
 *         *   RPC 框架的基础设施（如注册中心客户端、网络传输层）也已启动（因为它们也是 Spring Bean，且通常在此方法之前初始化）。
 *         *   避免了在 `@Autowired` 等标准注入机制无法覆盖的场景下（例如需要复杂的配置构建过程）导致注入失败。
 *
 * ---
 *
 *  3. 辅助方法与内部类
 *
 * *   `addAnnotationConsumer` / `addProvider`: 将扫描到的注解信息转换为内部的 Config 对象，并处理名称生成和冲突检查。
 * *   `getInterfaceClass`: 智能推断服务提供者实现的接口。如果实现了多个接口，会根据命名规则（如类名以接口名开头）或优先级（排除 java/javax 开头的接口）选择最合适的接口。
 * *   `AnnotationFilter`: Spring 扫描器使用的过滤器。它通过反射读取类元数据，判断该类是否包含目标注解，从而决定是否将其纳入候选组件。
 *
 *  总结
 *
 * `RpcDefinitionPostProcessor` 是连接 Spring 生命周期和 RPC 框架的桥梁。
 * 1.  它在启动早期扫描和注册基础设施 Bean。
 * 2.  它在启动中期解析业务代码中的注解，生成 RPC 配置对象。
 * 3.  它在启动后期业务 Bean 初始化完成后，拦截它们，将远程服务的代理对象注入进去，从而实现“像调用本地服务一样调用远程服务”的透明化体验。
 *
 * 你关注的 `postProcessAfterInitialization` 方法正是这最后一步的关键执行者，它利用反射将第一阶段准备好的配置“变现”为实际的代理对象并注入到业务 Bean 中。
 */

/**
 * 注解扫描处理类
 */
public class RpcDefinitionPostProcessor implements BeanDefinitionRegistryPostProcessor,
        BeanPostProcessor, BeanClassLoaderAware {

    /**
     * 服务名称
     */
    public static final String SERVER_NAME = "server";
    /**
     * 注册中心名称
     */
    public static final String REGISTRY_NAME = "registry";

    public static final String BEAN_NAME = "rpcDefinitionPostProcessor";
    public static final String RPC_PREFIX = "rpc";
    public static final String PROVIDER_PREFIX = "provider-";
    public static final String CONSUMER_PREFIX = "consumer-";
    public static final String REF_PREFIX = "ref:";
    public static final String REF_PREFIX_KEY = "rpc.ref.prefix";

    protected final ConfigurableEnvironment environment;

    protected final ResourceLoader resourceLoader;

    protected final ApplicationContext applicationContext;

    protected RpcProperties rpcProperties;

    protected ClassLoader classLoader;
    /**
     * ConsumerBean集合
     */
    protected Map<String, ConsumerBean<?>> consumers = new HashMap<>();
    /**
     * ConsumerGroupBean集合
     */
    protected Map<String, ConsumerGroupBean<?>> groups = new HashMap<>();
    /**
     * ProviderBean 集合
     */
    protected Map<String, ProviderBean<?>> providers = new HashMap<>();
    /**
     * 字段或方法上对应的消费者
     */
    protected Map<Member, AbstractConsumerConfig<?>> members = new HashMap<>();
    /**
     * Consumer名称计数器
     */
    protected Map<String, AtomicInteger> consumerNameCounters = new HashMap<>();
    /**
     * Provider名称计数器
     */
    protected Map<String, AtomicInteger> providerNameCounters = new HashMap<>();
    /**
     * 引用前缀
     */
    protected String refPrefix;
    /**
     * 服务bean计数器
     */
    protected transient Counter counter;

    /**
     * 构造方法
     */
    public RpcDefinitionPostProcessor(final ApplicationContext applicationContext,
                                      final ConfigurableEnvironment environment,
                                      final ResourceLoader resourceLoader) {
        this.applicationContext = applicationContext;
        this.environment = environment;
        this.resourceLoader = resourceLoader;
        this.counter = Counter.getOrCreate(applicationContext);
        //值引用前缀
        this.refPrefix = environment.getProperty(REF_PREFIX_KEY, REF_PREFIX);
    }

    @Override
    public void postProcessBeanDefinitionRegistry(final BeanDefinitionRegistry registry) throws BeansException {
        //避免放在构造函数里面，因为有些bean还没有定义好
        this.rpcProperties = Binder.get(environment).bind(RPC_PREFIX, RpcProperties.class).orElseGet(RpcProperties::new);
        //添加消费者
        if (rpcProperties.getConsumers() != null) {
            rpcProperties.getConsumers().forEach(c -> addConfig(c, CONSUMER_PREFIX, consumerNameCounters, consumers));
        }
        //添加消费组
        if (rpcProperties.getGroups() != null) {
            rpcProperties.getGroups().forEach(c -> addConfig(c, CONSUMER_PREFIX, consumerNameCounters, groups));
        }
        //添加服务提供者
        if (rpcProperties.getProviders() != null) {
            rpcProperties.getProviders().forEach(c -> addConfig(c, PROVIDER_PREFIX, providerNameCounters, providers));
        }

        //扫描知道包下面的消费者和服务提供者注解
        Set<String> packages = new LinkedHashSet<>();
        if (rpcProperties.getPackages() != null) {
            rpcProperties.getPackages().forEach(pkg -> {
                if (hasText(pkg)) {
                    packages.add(pkg.trim());
                }
            });
        }
        processPackages(packages, registry);
        //注册Bean
        register(registry);
    }

    @Override
    public void postProcessBeanFactory(final ConfigurableListableBeanFactory beanFactory) throws BeansException {

    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
        //再次查找所有的Consumer注解，检查是否注入了。
        processConsumerAnnotation(bean.getClass(),
                (f, c) -> {
                    AbstractConsumerConfig<?> config = members.get(f);
                    if (config != null) {
                        ReflectionUtils.makeAccessible(f);
                        ReflectionUtils.setField(f, bean, config.proxy());
                    }
                },
                (m, c) -> {
                    AbstractConsumerConfig<?> config = members.get(m);
                    if (config != null) {
                        ReflectionUtils.invokeMethod(m, bean, config.proxy());
                    }
                });
        return bean;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * 添加配置者
     *
     * @param config   配置
     * @param prefix   前缀
     * @param counters 计数器
     * @param configs  配置容器
     */
    protected <T extends AbstractInterfaceConfig> void addConfig(final T config,
                                                                 final String prefix,
                                                                 final Map<String, AtomicInteger> counters,
                                                                 final Map<String, T> configs) {
        if (config == null) {
            return;
        }
        String name = computeName(config, prefix, counters);
        if (!isEmpty(name)) {
            if (configs.putIfAbsent(name, config) != null) {
                //名称冲突
                throw new BeanInitializationException("duplication bean name " + name);
            }
        }
    }

    /**
     * 计算名称
     *
     * @param config   配置
     * @param prefix   前缀
     * @param counters 计数器
     * @param <T>
     * @return
     */
    protected <T extends AbstractInterfaceConfig> String computeName(final T config,
                                                                     final String prefix,
                                                                     final Map<String, AtomicInteger> counters) {
        String name = config.getId();
        String interfaceClazz = config.getInterfaceClazz();
        if (isEmpty(name) && !isEmpty(interfaceClazz)) {
            name = prefix + Introspector.decapitalize(getShortName(interfaceClazz));
            if (counters != null) {
                AtomicInteger counter = counters.computeIfAbsent(name, n -> new AtomicInteger(0));
                int index = counter.incrementAndGet();
                name = index == 1 ? name : name + "-" + index;
            }
            config.setId(name);
        }
        return name;
    }

    /**
     * 通过注解添加消费者
     *
     * @param consumer       消费者配置
     * @param interfaceClazz 接口类
     */
    protected AbstractConsumerConfig<?> addAnnotationConsumer(final ConsumerBean<?> consumer, final Class<?> interfaceClazz) {
        consumer.setInterfaceClass(interfaceClazz);
        consumer.setInterfaceClazz(interfaceClazz.getName());
        //注解的不自动添加计数器
        String name = computeName(consumer, CONSUMER_PREFIX, null);
        //先处理消费组的配置
        ConsumerGroupBean<?> groupBean = groups.get(name);
        if (groupBean != null) {
            //定义了消费组
            groupBean.setInterfaceClazz(consumer.getInterfaceClazz());
            groupBean.setInterfaceClass(interfaceClazz);
            if (isEmpty(groupBean.getAlias())) {
                groupBean.setAlias(consumer.getAlias());
            }
            return groupBean;
        } else {
            //普通消费者
            ConsumerBean<?> old = consumers.putIfAbsent(name, consumer);
            if (old != null) {
                old.setInterfaceClazz(consumer.getInterfaceClazz());
                old.setInterfaceClass(interfaceClazz);
                if (isEmpty(old.getAlias())) {
                    old.setAlias(consumer.getAlias());
                }
            }
            return old != null ? old : consumer;
        }
    }

    /**
     * 通过注解添加服务提供者
     *
     * @param provider       服务提供者
     * @param interfaceClazz 接口类
     * @param refName        引用对象
     */
    protected ProviderBean<?> addProvider(final ProviderBean<?> provider, final Class<?> interfaceClazz, final String refName) {
        //这里不为空
        provider.setInterfaceClass(interfaceClazz);
        provider.setInterfaceClazz(interfaceClazz.getName());
        provider.setRefName(refName);
        //注解的不自动添加计数器
        ProviderBean<?> old = providers.putIfAbsent(provider.getId(), provider);
        if (old != null) {
            if (isEmpty(old.getInterfaceClazz())) {
                old.setInterfaceClazz(provider.getInterfaceClazz());
            }
            if (isEmpty(old.getAlias())) {
                old.setAlias(provider.getAlias());
            }
            if (isEmpty(old.getRefName())) {
                old.setRefName(refName);
            }
        }
        return old != null ? old : provider;
    }


    /**
     * 注册
     */
    protected void register(final BeanDefinitionRegistry registry) {
        //注册全局参数
        Map<String, String> parameters = rpcProperties.getParameters();
        if (parameters != null) {
            //从配置文件读取，值已经做了占位符替换
            parameters.forEach((key, value) -> {
                if (value != null && value.startsWith(refPrefix)) {
                    String ref = value.substring(refPrefix.length());
                    if (!StringUtils.isEmpty(ref)) {
                        GlobalParameterDefinitionParser.register(registry, counter, key, null, ref, null);
                    }
                }
                GlobalParameterDefinitionParser.register(registry, counter, key, value);
            });
        }
        //注册
        String defRegName = register(registry, rpcProperties.getRegistry(), REGISTRY_NAME);
        String defServerName = register(registry, rpcProperties.getServer(), SERVER_NAME);
        register(registry, rpcProperties.getRegistries(), REGISTRY_NAME);
        register(registry, rpcProperties.getServers(), SERVER_NAME);
        consumers.forEach((name, c) -> register(c, registry, defRegName));
        groups.forEach((name, c) -> register(c, registry, defRegName));
        providers.forEach((name, p) -> register(p, registry, defRegName, defServerName));
    }

    /**
     * 注册消费者
     *
     * @param config     消费者配置
     * @param registry   BeanDefinitionRegistry
     * @param defRegName 默认注册中心
     */
    protected void register(final ConsumerBean<?> config, final BeanDefinitionRegistry registry, final String defRegName) {
        BeanDefinitionBuilder builder = genericBeanDefinition(ConsumerBean.class, () -> config)
                .setRole(RootBeanDefinition.ROLE_INFRASTRUCTURE);

        //这些不需要被再次Proxy，设置成ROLE_INFRASTRUCTURE，忽略Spring的警告
        if (config.getRegistry() == null
                && isEmpty(config.getRegistryName())
                && !isEmpty(defRegName)) {
            //引用registry
            config.setRegistryName(defRegName);
        }
        //注册
        registry.registerBeanDefinition(config.getName(), builder.getBeanDefinition());
    }

    /**
     * 注册消费者
     *
     * @param config     消费组配置
     * @param registry   BeanDefinitionRegistry
     * @param defRegName 默认注册中心
     */
    protected void register(final ConsumerGroupBean<?> config, final BeanDefinitionRegistry registry, final String defRegName) {
        BeanDefinitionBuilder builder = genericBeanDefinition(ConsumerGroupConfig.class, () -> config)
                .setRole(RootBeanDefinition.ROLE_INFRASTRUCTURE);
        //这些不需要被再次Proxy，设置成ROLE_INFRASTRUCTURE，忽略Spring的警告
        if (config.getRegistry() == null
                && isEmpty(config.getRegistryName())
                && !isEmpty(defRegName)) {
            //引用registry
            config.setRegistryName(defRegName);
        }
        //注册
        registry.registerBeanDefinition(config.getName(), builder.getBeanDefinition());
    }

    /**
     * 注册服务提供者
     *
     * @param config        服务提供者配置
     * @param registry      注册中心
     * @param defRegName    默认注册中心引用
     * @param defServerName 默认网络服务引用
     */
    protected void register(final ProviderBean<?> config, final BeanDefinitionRegistry registry, final String defRegName,
                            final String defServerName) {
        //这些不需要被再次Proxy，设置成ROLE_INFRASTRUCTURE，忽略Spring的警告
        BeanDefinitionBuilder builder = genericBeanDefinition(ProviderBean.class, () -> config)
                .setRole(RootBeanDefinition.ROLE_INFRASTRUCTURE);
        //判断是否设置了注册中心配置
        if (CollectionUtils.isEmpty(config.getRegistry())
                && CollectionUtils.isEmpty(config.getRegistryNames())
                && !isEmpty(defRegName)) {
            //引用注册中心
            config.setRegistryNames(Arrays.asList(defRegName));
        }
        //引用Server
        if (config.getServerConfig() == null
                && isEmpty(config.getServerName())
                && !isEmpty(defServerName)) {
            config.setServerName(defServerName);
        }
        //注册
        registry.registerBeanDefinition(config.getName(), builder.getBeanDefinition());
    }

    /**
     * 处理rpc扫描的包下的class类
     *
     * @param packages 包集合
     * @param registry 注册中心
     */
    protected void processPackages(Set<String> packages, BeanDefinitionRegistry registry) {
        //构造
        ClassPathBeanDefinitionScanner scanner = new ClassPathBeanDefinitionScanner(registry, false, environment, resourceLoader);
        registerAnnotationConfigProcessors(registry);
        scanner.addIncludeFilter(new AnnotationFilter());
        //获取配置的rpc扫描包下的所有bean定义
        for (String basePackage : packages) {
            Set<BeanDefinition> definitions = scanner.findCandidateComponents(basePackage);
            if (!CollectionUtils.isEmpty(definitions)) {
                for (BeanDefinition definition : definitions) {
                    processConsumerAnnotation(definition);
                    processProviderAnnotation(definition, registry);
                }
            }
        }

    }

    /**
     * 处理消费者注解
     *
     * @param definition bean定义
     */
    protected void processConsumerAnnotation(final BeanDefinition definition) {
        String className = definition.getBeanClassName();
        if (!isEmpty(className)) {
            Class<?> beanClass = resolveClassName(className, classLoader);
            processConsumerAnnotation(beanClass,
                    (f, c) -> members.put(f, addAnnotationConsumer(c, f.getType())),
                    (m, c) -> members.put(m, addAnnotationConsumer(c, m.getParameterTypes()[1])));
        }
    }

    /**
     * 处理消费者注解
     *
     * @param beanClass      bean类
     * @param fieldConsumer  字段消费者
     * @param methodConsumer 方法消费者
     */
    protected void processConsumerAnnotation(final Class<?> beanClass,
                                             final BiConsumer<Field, ConsumerBean<?>> fieldConsumer,
                                             final BiConsumer<Method, ConsumerBean<?>> methodConsumer) {

        Class<?> targetClass = beanClass;
        Pair<AnnotationProvider<Annotation, Annotation>, Annotation> pair;
        while (targetClass != null && targetClass != Object.class) {
            //处理字段上的注解
            for (Field field : targetClass.getDeclaredFields()) {
                if (!Modifier.isFinal(field.getModifiers()) && !Modifier.isStatic(field.getModifiers())) {
                    pair = getConsumerAnnotation(field);
                    if (pair != null) {
                        fieldConsumer.accept(field, pair.getKey().toConsumerBean(pair.getValue(), environment));
                    }
                }
            }
            //处理方法上的注解
            for (Method method : targetClass.getDeclaredMethods()) {
                if (!Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers())
                        && method.getParameterCount() == 1
                        && method.getName().startsWith("set")) {
                    pair = getConsumerAnnotation(method);
                    if (pair != null) {
                        methodConsumer.accept(method, pair.getKey().toConsumerBean(pair.getValue(), environment));
                    }
                }
            }
            targetClass = targetClass.getSuperclass();
        }
    }

    /**
     * 处理服务提供者注解
     *
     * @param definition bean定义
     * @param registry   注册中心
     */
    protected void processProviderAnnotation(final BeanDefinition definition, BeanDefinitionRegistry registry) {
        String className = definition.getBeanClassName();
        if (isEmpty(className)) {
            return;
        }
        Class<?> providerClass = resolveClassName(className, classLoader);
        //查找服务提供者注解
        Class<?> targetClass = providerClass;
        Pair<AnnotationProvider<Annotation, Annotation>, Annotation> pair = null;
        while (targetClass != null && targetClass != Object.class) {
            pair = getProviderAnnotation(targetClass);
            if (pair != null) {
                break;
            }
            targetClass = targetClass.getSuperclass();
        }
        if (pair != null) {
            ProviderBean<?> provider = pair.getKey().toProviderBean(pair.getValue(), environment);
            //获取接口类名
            Class<?> interfaceClazz = provider.getInterfaceClass(() -> getInterfaceClass(providerClass));
            if (interfaceClazz == null) {
                //没有找到接口
                throw new BeanInitializationException("there is not any interface in class " + providerClass);
            }
            //获取服务实现类的Bean名称
            String refName = getComponentName(providerClass);
            if (refName == null) {
                refName = Introspector.decapitalize(getShortName(providerClass.getName()));
                //注册服务实现对象
                if (!registry.containsBeanDefinition(refName)) {
                    registry.registerBeanDefinition(refName, definition);
                }
            }
            refName = isEmpty(refName) ? Introspector.decapitalize(getShortName(providerClass.getName())) : refName;
            if (isEmpty(provider.getId())) {
                provider.setId(PROVIDER_PREFIX + refName);
            }
            //添加provider
            addProvider(provider, interfaceClazz, refName);
        }
    }

    /**
     * 获取组件名称
     *
     * @param providerClass 服务提供接口类
     * @return 服务名称
     */
    protected String getComponentName(final Class<?> providerClass) {
        String name = null;
        Component component = findAnnotation(providerClass, Component.class);
        if (component != null) {
            name = component.value();
        }
        if (isEmpty(name)) {
            Service service = findAnnotation(providerClass, Service.class);
            if (service != null) {
                name = service.value();
            }
        }
        return name;
    }

    /**
     * 获取接口
     *
     * @param providerClass 服务提供者类
     * @return 服务接口类
     */
    protected Class<?> getInterfaceClass(final Class<?> providerClass) {
        Class<?> interfaceClazz = null;
        Class<?>[] interfaces = providerClass.getInterfaces();
        if (interfaces.length == 1) {
            interfaceClazz = interfaces[0];
        } else if (interfaces.length > 1) {
            //多个接口，查找最佳匹配接口
            int max = -1;
            int priority;
            String providerClassName = providerClass.getSimpleName();
            String intfName;
            //计算最佳接口
            for (Class<?> intf : interfaces) {
                intfName = intf.getName();
                if (intfName.startsWith("java")) {
                    priority = 0;
                } else if (intfName.startsWith("javax")) {
                    priority = 0;
                } else {
                    priority = providerClassName.startsWith(intf.getSimpleName()) ? 2 : 1;
                }
                if (priority > max) {
                    interfaceClazz = intf;
                    max = priority;
                }
            }
        }
        return interfaceClazz;
    }


    /**
     * 注册
     *
     * @param registry      注册表
     * @param configs       多个配置
     * @param defNamePrefix 默认名称
     */
    protected <T extends AbstractIdConfig> void register(final BeanDefinitionRegistry registry,
                                                         final List<T> configs, final String defNamePrefix) {
        if (configs != null) {
            AtomicInteger counter = new AtomicInteger(0);
            for (T config : configs) {
                register(registry, config, defNamePrefix + "-" + counter.getAndIncrement());
            }
        }
    }

    /**
     * 注册
     *
     * @param registry BeanDefinitionRegistry
     * @param config   配置
     * @param defName  默认名称
     * @param <T>
     */
    protected <T extends AbstractIdConfig> String register(final BeanDefinitionRegistry registry, final T config,
                                                           final String defName) {
        if (config == null) {
            return null;
        }
        String beanName = config.getId();
        if (isEmpty(beanName)) {
            beanName = defName;
        }
        if (!registry.containsBeanDefinition(beanName)) {
            RootBeanDefinition definition = new RootBeanDefinition((Class<T>) config.getClass(), () -> config);
            //避免Spring警告信息
            definition.setRole(RootBeanDefinition.ROLE_INFRASTRUCTURE);
            registry.registerBeanDefinition(beanName, definition);
        } else {
            throw new BeanInitializationException("duplication bean name " + beanName);
        }
        return beanName;
    }

    /**
     * 获取注解
     *
     * @param function 函数
     * @return 注解提供者和注解的键值对
     */
    protected Pair<AnnotationProvider<Annotation, Annotation>, Annotation> getAnnotation(final Function<AnnotationProvider<Annotation, Annotation>, Annotation> function) {
        Annotation result;
        for (AnnotationProvider<Annotation, Annotation> provider : ANNOTATION_PROVIDER.extensions()) {
            result = function.apply(provider);
            if (result != null) {
                return Pair.of(provider, result);
            }
        }
        return null;
    }

    /**
     * 获取类上的服务提供者注解
     *
     * @param clazz 类
     * @return 注解提供者和注解的键值对
     */
    protected Pair<AnnotationProvider<Annotation, Annotation>, Annotation> getProviderAnnotation(final Class<?> clazz) {
        return getAnnotation(p -> clazz.getDeclaredAnnotation(p.getProviderAnnotationClass()));
    }

    /**
     * 获取方法上的消费者注解
     *
     * @param method 方法
     * @return 注解提供者和注解的键值对
     */
    protected Pair<AnnotationProvider<Annotation, Annotation>, Annotation> getConsumerAnnotation(final Method method) {
        return getAnnotation(p -> method.getAnnotation(p.getConsumerAnnotationClass()));
    }

    /**
     * 获取字段上的消费者注解
     *
     * @param field 字段
     * @return 注解提供者和注解的键值对
     */
    protected Pair<AnnotationProvider<Annotation, Annotation>, Annotation> getConsumerAnnotation(final Field field) {
        return getAnnotation(p -> field.getAnnotation(p.getConsumerAnnotationClass()));
    }

    /**
     * 扫描类过滤（主要用来过滤含有某一个注解的类）
     */
    protected class AnnotationFilter implements TypeFilter {

        @Override
        public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) {
            ClassMetadata classMetadata = metadataReader.getClassMetadata();
            if (classMetadata.isConcrete() && !classMetadata.isAnnotation()) {
                //找到类
                Class<?> clazz = resolveClassName(classMetadata.getClassName(), classLoader);
                //判断是否Public
                if (Modifier.isPublic(clazz.getModifiers())) {
                    Class<?> targetClass = clazz;
                    while (targetClass != null && targetClass != Object.class) {
                        //处理类上的服务提供者注解
                        if (getProviderAnnotation(targetClass) != null) {
                            return true;
                        }
                        //处理字段的消费者注解
                        for (Field field : targetClass.getDeclaredFields()) {
                            if (!Modifier.isFinal(field.getModifiers())
                                    && !Modifier.isStatic(field.getModifiers())
                                    && getConsumerAnnotation(field) != null) {
                                return true;
                            }
                        }
                        //处理方法上的消费者注解
                        for (Method method : clazz.getDeclaredMethods()) {
                            if (!Modifier.isStatic(method.getModifiers())
                                    && Modifier.isPublic(method.getModifiers())
                                    && method.getParameterCount() == 1
                                    && method.getName().startsWith("set")
                                    && getConsumerAnnotation(method) != null) {
                                return true;
                            }
                        }
                        targetClass = targetClass.getSuperclass();
                    }

                }
            }
            return false;
        }
    }


}
