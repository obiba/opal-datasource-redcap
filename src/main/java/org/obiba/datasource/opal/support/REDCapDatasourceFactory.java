package org.obiba.datasource.opal.support;

import org.obiba.datasource.opal.REDCapDatasource;
import org.obiba.magma.AbstractDatasourceFactory;
import org.obiba.magma.Datasource;

public class REDCapDatasourceFactory extends AbstractDatasourceFactory {

	@Override
	protected Datasource internalCreate() {
		return new REDCapDatasource(getName());
	}

}