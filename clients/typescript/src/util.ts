
export function wait(timeout: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, timeout);
    });
}

export class AsyncFlag {
    private readonly flag: Promise<void>;
    private resolved = false;
    set = () => {};

    constructor() {
        this.flag = new Promise<void>((resolve) => {
           this.set = () => {
               this.resolved = true;
               resolve();
           };
        });
    }

    get isSet() {
        return this.resolved;
    }

    async wait(timeout?: number) {
        const promises = [this.flag];
        if (timeout) {
            promises.push(new Promise((_, reject) => {
                setTimeout(() => {
                    reject('Timeout');
                }, timeout);
            }));
        }
        return await Promise.race(promises);
    }
}


export class AsyncQueue<T> {
    private resolveNotifier = () => {};
    private queue: T[] = [];
    private closed = false;

    private createNotifier = () => {
        return new Promise<void>((resolve, reject) => {
            this.resolveNotifier = resolve;
            if (this.queue.length > 0) {
                resolve();
            }
        });
    }

    private notifier = this.createNotifier();

    push = (data: T) => {
        if (this.closed) {
            return;
        }
        this.queue.push(data);
        this.resolveNotifier();
    }

    async get (nowait: boolean = false): Promise<T | null> {
        if (nowait) {
            if (this.queue.length > 0) {
                const item = this.queue.shift()!;
                this.notifier = this.createNotifier();
                return item;
            }
            return null;
        }

        if (this.closed && !this.queue.length) {
            return null;
        }

        // Block by waiting for the notifier to resolve
        await this.notifier
        return this.get(true);
    }

    get length() {
        return this.queue.length;
    }

    close() {
        this.closed = true;
        this.resolveNotifier();
    }

}
