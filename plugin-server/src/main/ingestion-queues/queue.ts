import Piscina from '@posthog/piscina'
import { PluginEvent } from '@posthog/plugin-scaffold'

import { Hub, PostIngestionEvent } from '../../types'
import { status } from '../../utils/status'
import { KafkaQueue } from './kafka-queue'

export function pauseQueueIfWorkerFull(
    pause: undefined | (() => void | Promise<void>),
    server: Hub,
    piscina?: Piscina
): void {
    if (pause && (piscina?.queueSize || 0) > (server.WORKER_CONCURRENCY || 4) * (server.WORKER_CONCURRENCY || 4)) {
        void pause()
    }
}

export async function startKafkaConsumer(server: Hub, piscina: Piscina): Promise<KafkaQueue> {
    status.info('🔄', 'Starting ingestion/async handler consumer...')

    const workerMethods = {
        runAsyncHandlersEventPipeline: (event: PostIngestionEvent) => {
            server.lastActivity = new Date().valueOf()
            server.lastActivityType = 'runAsyncHandlersEventPipeline'
            return piscina.run({ task: 'runAsyncHandlersEventPipeline', args: { event } })
        },
        runEventPipeline: (event: PluginEvent) => {
            server.lastActivity = new Date().valueOf()
            server.lastActivityType = 'runEventPipeline'
            return piscina.run({ task: 'runEventPipeline', args: { event } })
        },
    }

    try {
        const queue = new KafkaQueue(server, workerMethods)
        await queue.start()
        return queue
    } catch (error) {
        status.error('💥', 'Failed to start event queue:\n', error)
        throw error
    }
}
