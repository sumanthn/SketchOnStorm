package sn.utils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Acts as a cache for Hosts names
 * which are keys to sketch
 * Created by sumanthn on 5/4/14.
 */
public class HostNameRepo {

    private static HostNameRepo ourInstance = new HostNameRepo();
    String[] namesPrefix = new String[]{
            "Web",
            "Db",
            "App",
            "Gateway",
            "Tapal", //mail exchange
            "Niyantraka", //Management & Monitoring
            "Backup"
    };
    private String hostNameSuffix = ".funkyfashion.com";

    private ImmutableList<String> hostNames;

    private HostNameRepo() {
    }

    public static HostNameRepo getInstance() {
        return ourInstance;
    }

    public void generateNames(int maxCount) {

        List<String> generatedNames = new ArrayList<String>();

        //4 gateways
        //8 Tapals, 4 Niyantrak , 20 Backup
        //rest Web,Db, App

        addHostNames(ServerTypes.GATEWAY.toString(), 4, generatedNames);
        addHostNames(ServerTypes.TAPAL.toString(), 8, generatedNames);
        addHostNames(ServerTypes.Niyantraka.toString(), 4, generatedNames);
        addHostNames(ServerTypes.BACKUP.toString(), 24, generatedNames);

        int remainingServerCount = maxCount - 40;
        Random random = new Random();
        random.setSeed(101201l);

        for (int i = 0; i < remainingServerCount; i++) {
            generatedNames.add(ServerTypes.values()[random.nextInt(3)].toString() + i + hostNameSuffix);
        }
        hostNames = ImmutableList.copyOf(generatedNames);
    }

    private void addHostNames(final String label, int count, List<String> names) {

        for (int i = 0; i < count; i++) {
            names.add(label + i + hostNameSuffix);
        }
    }

    enum ServerTypes {
        WEB,
        DB,
        APP,
        GATEWAY,
        TAPAL,//mail exchange
        Niyantraka,//Management & Monitoring
        BACKUP,
        FALTU; //unknown, beekar, useless

        public String toString() {
            switch (this) {

                case WEB:
                    return "web";

                case DB:
                    return "db";

                case APP:
                    return "app";

                case GATEWAY:
                    return "gateway";

                case TAPAL:
                    return "tapalu";

                case Niyantraka:
                    return "niyantraka";

                case BACKUP:
                    return "backup";

                case FALTU:
                    return "faltu";
            }
            return "faltu";
        }

    }

    public static void main(String[] args) {
        getInstance().generateNames(500);
    }

    public ImmutableList<String> getHostNameKeys() {
        return hostNames;
    }
}
