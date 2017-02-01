package inex09;

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import freebase.FreebaseDatabaseSizeExperiment;

public class InexExperimentWithPageCount {

	static final Logger LOGGER = Logger.getLogger(FreebaseDatabaseSizeExperiment.class.getName());

	public static void main(String[] args) {
		// initializations
		LOGGER.setUseParentHandlers(false);
		Handler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
		LOGGER.setLevel(Level.ALL);

	}


}
