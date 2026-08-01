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

    addShade = (colorIndex: number) => {
        const current =  this.params().colors.find((_, i)=>i === colorIndex);

        if(!current) {
            return;
        }

        this.params.set({
            ...this.params(),
            colors: [
                ...this.params().colors.filter((_, i)=>i != colorIndex),
                {
                   hue: current?.hue,
                   shades: [
                        ...current?.shades,
                        0.5
                   ]
                }
            ]
        })
    }

    addColor = () => {
        this.params.set({
            ...this.params(),
            colors: [
                ...this.params().colors,
                {
                    hue: 15,
                    shades: [0.25, 0.45, 0.65, 0.85]
                }
            ]
        })
    }

    removeShade = (colorIndex: number, shadeIndex: number) => {
        const current =  this.params().colors.find((_, i)=>i === colorIndex);

        if(!current) {
            return;
        }

        this.params.set({
            ...this.params(),
            colors: [
                ...this.params().colors.filter((_, i)=>i != colorIndex),
                {
                   hue: current?.hue,
                   shades: [
                        ...current?.shades.filter((_, i)=>i != shadeIndex),
                   ]
                }
            ]
        })
    }

    removeColor = (colorIndex: number) => {
        const current =  this.params().colors.find((_, i)=>i === colorIndex);

        if(!current) {
            return;
        }

        this.params.set({
            ...this.params(),
            colors: [
                ...this.params().colors.filter((_, i)=>i != colorIndex)
            ]
        })
    }
}
