import { computed, effect, inject, Service, signal } from '@angular/core';
import { applyEach, disabled, form, max, min } from '@angular/forms/signals';
import { UtilsService } from './utils.service';
import { untracked } from '@angular/core/primitives/signals';

interface StateParamsColor {
    id: number,
    hue: number,
    hueDiff: number,
    lockHue: boolean,
    saturation: number,
    lockSaturation: boolean,
    shades: number[],
    lockShade: boolean
}

interface StateParams {
    colors: StateParamsColor[];
}

@Service()
export class StateService {
    utilsService = inject(UtilsService)

    inc = signal<number>(0)

    params = signal<StateParams>({
        colors: [
            this._createColor()
        ]
    })

    stateForm = form(
        this.params,
        (path) => {
            applyEach(path.colors, (colorPath) => {
                applyEach(colorPath.shades, (shadePath) => {
                    min(shadePath, 0);
                    max(shadePath, 1);
                    disabled(shadePath, {
                        when: ({ valueOf }) => valueOf(colorPath.lockShade),
                    })
                });
            });
        }
    )

    _ = effect(() => {
        this._updateColors(this.params())
    })

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
                    ...current,
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
                this._createColor()
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
                   ...current,
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

    private _updateColors(params: StateParams) {
        const colors = params.colors

        const mainColor = colors[0];

        if (colors.length > 1) {        
            for (let i = 1; i < colors.length; i++) {
                const currentColor = colors[i];
                if(currentColor.lockHue) {
                    currentColor.hue = this.utilsService.addHue(mainColor.hue, currentColor.hueDiff)
                }
                else {
                    currentColor.hueDiff = currentColor.hue - mainColor.hue
                }

                if(currentColor.lockSaturation) {
                    currentColor.saturation = mainColor.saturation
                }

                if(currentColor.lockShade) {
                    currentColor.shades = [...mainColor.shades]
                }
            }
        }
    }

    private _createColor():StateParamsColor {
        const firstColor = this.params?.().colors[0];
        if (firstColor) {
            return {
                id: this._increment(),
                hue: this.utilsService.addHue(firstColor.hue, 30),
                hueDiff: 30,
                lockHue: true,
                saturation: firstColor.saturation,
                lockSaturation: true,
                shades: [
                    ...firstColor.shades
                ],
                lockShade: true
            }
        }

        return {
            id: this._increment(),
            hue: 15, 
            hueDiff: 0,
            lockHue: false,
            saturation: 0.52,
            lockSaturation: false,
            shades: [
                0.23, 0.43, 0.63, 0.83
            ],
            lockShade: false
        }
    }

    private _increment() {
        const cur = this.inc()
        this.inc.set(cur+1);
        return cur;
    }
}
