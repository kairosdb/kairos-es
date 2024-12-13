package org.kairosdb.plugin.es;

import com.google.inject.AbstractModule;

public class EsModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(RowKeyListener.class).asEagerSingleton();
	}
}
