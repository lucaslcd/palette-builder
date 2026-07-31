import { computed, Service, signal } from '@angular/core';
import { applyEach, form, max, min } from '@angular/forms/signals';

interface StateParamsColor {
    hue: number,
    shades: number[]
}

interface StateParams {
    saturation: number;
    colors: StateParamsColor[];
}

@Service()
export class StateService {
    params = signal<StateParams>({
        saturation: 0.46,
        colors: [
            {
                hue: 15,
                shades: [0.25, 0.45, 0.65, 0.85]
            }
        ]
    })

    stateForm = form(
        this.params,
        (path) => {
            applyEach(path.colors, (huePath) => {
                applyEach(huePath.shades, (shadePath) => {
                    min(shadePath, 0);
                    max(shadePath, 1);
                });
            });
        }
    )
}
