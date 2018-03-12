package com.elasticm2m.frameworks.test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

public abstract class ComponentTest<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(ComponentTest.class);

    protected final Class<T> componentType;

    @Mock
    protected TopologyContext topologyContext;

    @Mock
    protected OutputCollector boltCollector;

    @Mock
    protected SpoutOutputCollector spoutCollector;

    protected ArgumentCaptor<List> tupleCaptor = ArgumentCaptor.forClass(List.class);

    public ComponentTest() {
        this.componentType = ((Class) ((ParameterizedType) this.getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    protected T mockComponent() {
        return mockComponent(new ArrayList<Module>());
    }

    protected T mockComponent(Module module) {
        List<Module> modules = new ArrayList<>();
        modules.add(module);
        return mockComponent(modules);
    }

    protected T mockComponent(List<Module> modules) {
        if (modules == null) {
            modules = new ArrayList<>();
        }

        // Initialize the core module used to provide common functionality for all components
        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Logger.class).toInstance(LOG);
                bindConstant().annotatedWith(
                        Names.named("streamflow.component.label")).to(componentType.getSimpleName());
                bindConstant().annotatedWith(
                        Names.named("streamflow.component.name")).to(componentType.getSimpleName());
            }
        });

        // Initialize the injector for injection of required bolt properties
        Injector injector = Guice.createInjector(modules);

        // Inject and prepare the bolt for execution
        return injector.getInstance(componentType);
    }
}
