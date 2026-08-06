import { Service, signal } from '@angular/core';
import { form } from '@angular/forms/signals';

interface GlobalStateModel {
    display: {
        color: string,
        form: string
    }
}
@Service()
export class GlobalState {        

    state = signal<GlobalStateModel>({
        display: {
            color: 'wheel',
            form: 'edit'
        },
    })

    stateForm = form(
        this.state
    )
}
