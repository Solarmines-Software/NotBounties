package me.jadenp.notbounties;

import com.cjcrafter.foliascheduler.TaskImplementation;
import me.jadenp.notbounties.features.settings.display.map.BountyBoard;
import me.jadenp.notbounties.features.LanguageOptions;
import me.jadenp.notbounties.features.settings.money.NumberFormatting;
import me.jadenp.notbounties.features.settings.display.WantedTags;
import org.bukkit.Chunk;
import org.bukkit.Location;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Entity;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.persistence.PersistentDataContainer;
import org.bukkit.persistence.PersistentDataType;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static me.jadenp.notbounties.NotBounties.*;

public class RemovePersistentEntitiesEvent implements Listener {
    // Use WeakHashMap to prevent memory leak - chunks are automatically removed when unloaded
    private static final Map<Chunk, Boolean> completedChunks = Collections.synchronizedMap(new WeakHashMap<>());
    private static final List<Entity> removedEntities = Collections.synchronizedList(new ArrayList<>());
    // Queue for batching chunk processing to prevent thread exhaustion on Folia
    // Limit queue size to prevent memory leak if processing can't keep up
    private static final int MAX_QUEUE_SIZE = 1000; // Maximum chunks queued
    private static final Queue<List<Entity>> pendingChunkEntities = new ConcurrentLinkedQueue<>();
    private static volatile TaskImplementation<Void> batchProcessingTask = null;
    private static final Object batchTaskLock = new Object();
    private static final int BATCH_SIZE = 20; // Process up to 20 chunks per batch (increased for better throughput)
    private static final long BATCH_INTERVAL_TICKS = 3; // Process batches every 3 ticks (150ms) for faster processing

    @EventHandler
    public void onChunkLoad(ChunkLoadEvent event) {
        if (NotBounties.isPaused())
            return;
        // remove persistent entities (wanted tags & bounty boards)
        if (WantedTags.isEnabled() || !BountyBoard.getBountyBoards().isEmpty()) {
            // Queue the chunk for batch processing instead of processing immediately
            // This prevents creating too many async tasks when many chunks load at once
            List<Entity> entities = new ArrayList<>(Arrays.asList(event.getChunk().getEntities()));
            if (!entities.isEmpty()) {
                // Prevent memory leak by limiting queue size
                if (pendingChunkEntities.size() >= MAX_QUEUE_SIZE) {
                    // Queue is full - process immediately to prevent overflow
                    NotBounties.debugMessage("Chunk entity queue full, processing immediately", false);
                    cleanAsync(entities, null);
                } else {
                    pendingChunkEntities.offer(entities);
                    ensureBatchTaskRunning();
                }
            }
        }
    }
    
    private static void ensureBatchTaskRunning() {
        // Double-check locking pattern for thread safety
        if (batchProcessingTask == null) {
            synchronized (batchTaskLock) {
                if (batchProcessingTask == null && NotBounties.getInstance().isEnabled()) {
                    startBatchProcessingTask();
                }
            }
        }
    }
    
    private static void startBatchProcessingTask() {
        // Keep the task running continuously - don't cancel it to avoid race conditions
        // The task will just return early when there's nothing to process
        batchProcessingTask = NotBounties.getServerImplementation().global().runAtFixedRate(() -> {
            if (NotBounties.isPaused()) {
                return;
            }
            
            if (pendingChunkEntities.isEmpty()) {
                return; // Nothing to process, but keep task running
            }
            
            // Process a batch of chunks synchronously
            List<List<Entity>> batch = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE && !pendingChunkEntities.isEmpty(); i++) {
                List<Entity> entities = pendingChunkEntities.poll();
                if (entities != null) {
                    batch.add(entities);
                }
            }
            
            if (!batch.isEmpty()) {
                // Combine all entities from the batch and process them together synchronously
                List<Entity> combinedEntities = new ArrayList<>();
                for (List<Entity> entities : batch) {
                    combinedEntities.addAll(entities);
                }
                // Process synchronously - no async tasks created
                cleanAsync(combinedEntities, null);
            }
        }, 20, BATCH_INTERVAL_TICKS);
    }
    
    public static void shutdown() {
        synchronized (batchTaskLock) {
            if (batchProcessingTask != null) {
                batchProcessingTask.cancel();
                batchProcessingTask = null;
            }
        }
    }

    public static void checkRemovedEntities() {
        // Clean up invalid entities to prevent memory leak
        ListIterator<Entity> entityListIterator = removedEntities.listIterator();
        while (entityListIterator.hasNext()) {
            Entity entity = entityListIterator.next();
            if (!entity.isValid()) {
                entityListIterator.remove();
            } else {
                NotBounties.debugMessage("Entity was not removed", false);
                entity.remove();
            }
        }
        // Limit list size to prevent unbounded growth if entities aren't being invalidated
        // Keep only the most recent 1000 entities
        if (removedEntities.size() > 1000) {
            synchronized (removedEntities) {
                while (removedEntities.size() > 1000) {
                    Entity oldest = removedEntities.remove(0);
                    if (oldest.isValid()) {
                        try {
                            oldest.remove();
                        } catch (Exception e) {
                            // Entity might have been removed already
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks the chunk for bounty entities if the chunk is loaded.
     * @param location chunk location
     */
    public static void cleanChunk(Location location) {
        if (!NotBounties.getServerImplementation().isOwnedByCurrentRegion(location)) {
            NotBounties.getServerImplementation().region(location).run(() -> cleanChunk(location.getChunk()));
        } else {
            cleanChunk(location.getChunk());
        }
    }

    public static void cleanChunks(List<Location> locations) {
        locations.forEach(RemovePersistentEntitiesEvent::cleanChunk);
    }

    public static void cleanChunk(Chunk chunk) {
        if (completedChunks.containsKey(chunk))
            return;
        completedChunks.put(chunk, Boolean.TRUE);
        List<Entity> entities = new ArrayList<>(List.of(chunk.getEntities()));
        if (!entities.isEmpty()) {
            // Queue for batch processing instead of processing immediately
            // Prevent memory leak by limiting queue size
            if (pendingChunkEntities.size() >= MAX_QUEUE_SIZE) {
                // Queue is full - process immediately to prevent overflow
                NotBounties.debugMessage("Chunk entity queue full, processing immediately", false);
                cleanAsync(entities, null);
            } else {
                pendingChunkEntities.offer(entities);
                if (batchProcessingTask == null) {
                    startBatchProcessingTask();
                }
            }
        }
    }

    /**
     * Iterates through all the entities to find any persistent bounty entities.
     * Entities are then removed back on the main thread.
     * This method processes entities synchronously to avoid thread exhaustion on Folia.
     * @param entities Entities to check
     */
    public static void cleanAsync(List<Entity> entities, CommandSender sender) {
        // Process synchronously - the entity checking is lightweight and doesn't need async
        // This prevents thread exhaustion when many chunks load at once
        Set<UUID> activeEntities = WantedTags.getTagUUIDs();
        activeEntities.addAll(BountyBoard.getBoardUUIDs());
        List<Entity> toRemove = new ArrayList<>();
        for (Entity entity : entities) {
            if (!isBountyEntity(entity))
                continue;
            PersistentDataContainer container = entity.getPersistentDataContainer();
            if (container.has(getNamespacedKey(), PersistentDataType.STRING)) {
                String value = container.get(getNamespacedKey(), PersistentDataType.STRING);
                if (value != null) {
                    if (value.equals(SESSION_KEY)) {
                        if (!activeEntities.contains(entity.getUniqueId()))
                            toRemove.add(entity);
                    } else {
                        toRemove.add(entity);
                    }
                }

            }
        }

        removeSync(toRemove, sender);
    }

    private static boolean isBountyEntity(Entity entity) {
        return entity != null
                && (entity.getType() == EntityType.ARMOR_STAND || entity.getType() == EntityType.ITEM_FRAME
                || (NotBounties.getServerVersion() >= 17 && entity.getType() == EntityType.GLOW_ITEM_FRAME)
                || (NotBounties.isAboveVersion(19,3) && (entity.getType() == EntityType.TEXT_DISPLAY
                    || entity.getType() == EntityType.ITEM_DISPLAY || entity.getType() == EntityType.BLOCK_DISPLAY)));
    }

    /**
     * Synchronously remove entities from the server and send a response to the CommandSender.
     * Entities are batched by chunk to minimize task creation on Folia.
     * @param toRemove Entities to remove.
     * @param sender CommandSender to send a response to.
     */
    private static void removeSync(List<Entity> toRemove, @Nullable CommandSender sender) {
        if (toRemove.isEmpty() && sender == null)
            // nothing to remove and no message to be sent.
            return;
        
        // Batch entities by chunk to minimize task creation on Folia
        // All entities in the same chunk are in the same region, so we can batch them together
        Map<Chunk, List<Entity>> entitiesByChunk = new HashMap<>();
        for (Entity entity : toRemove) {
            if (entity != null && entity.isValid() && entity.getLocation().getChunk() != null) {
                Chunk chunk = entity.getLocation().getChunk();
                entitiesByChunk.computeIfAbsent(chunk, k -> new ArrayList<>()).add(entity);
            }
        }
        
        // Remove entities in batches per chunk (each chunk = one region task)
        for (Map.Entry<Chunk, List<Entity>> entry : entitiesByChunk.entrySet()) {
            Chunk chunk = entry.getKey();
            List<Entity> chunkEntities = entry.getValue();
            Location chunkLoc = chunk.getBlock(0, 0, 0).getLocation();
            NotBounties.getServerImplementation().region(chunkLoc).run(() -> {
                for (Entity entity : chunkEntities) {
                    if (entity.isValid()) {
                        try {
                            entity.remove();
                            removedEntities.add(entity);
                        } catch (Exception e) {
                            // Entity might have been removed already or world unloaded
                        }
                    }
                }
            });
        }
        
        if (sender != null) {
            if (sender instanceof Player player) {
                NotBounties.getServerImplementation().entity(player).run(() -> sender.sendMessage(LanguageOptions.parse(LanguageOptions.getPrefix() + LanguageOptions.getMessage("entity-remove").replace("{amount}", NumberFormatting.formatNumber(toRemove.size())), player)));
            } else {
                NotBounties.getServerImplementation().global().run(() -> sender.sendMessage(LanguageOptions.parse(LanguageOptions.getPrefix() + LanguageOptions.getMessage("entity-remove").replace("{amount}", NumberFormatting.formatNumber(toRemove.size())), null)));
            }
        }

    }
}

